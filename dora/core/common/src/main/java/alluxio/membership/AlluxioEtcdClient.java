package alluxio.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.netty.util.internal.StringUtil;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class AlluxioEtcdClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEtcdClient.class);
  private static final Lock INSTANCE_LOCK = new ReentrantLock();
  @GuardedBy("INSTANCE_LOCK")
  private static final AtomicReference<AlluxioEtcdClient> ALLUXIO_ETCD_CLIENT = new AtomicReference<>();
  protected AtomicBoolean mConnected = new AtomicBoolean(false);
  private Client mClient;
  public final ServiceDiscoveryRecipe mServiceDiscovery;
  public String[] mEndpoints = new String[0];
  private final Closer mCloser = Closer.create();
  // only watch for children change(add/remove) for given parent path
  private ConcurrentHashMap<String, Watch.Watcher> mRegisteredWatchers =
      new ConcurrentHashMap<>();

  public AlluxioEtcdClient(AlluxioConfiguration conf) {
    String clusterName = conf.getString(PropertyKey.ALLUXIO_CLUSTER_NAME);
    List<String> endpointsList = conf.getList(PropertyKey.ETCD_ENDPOINTS);
    mEndpoints = endpointsList.toArray(new String[endpointsList.size()]);
    mServiceDiscovery = new ServiceDiscoveryRecipe(this, clusterName);
  }

  public static AlluxioEtcdClient getInstance(AlluxioConfiguration conf) {
    if (ALLUXIO_ETCD_CLIENT.get() == null) {
      try (LockResource lockResource = new LockResource(INSTANCE_LOCK)) {
        if (ALLUXIO_ETCD_CLIENT.get() == null) {
          ALLUXIO_ETCD_CLIENT.set(new AlluxioEtcdClient(conf));
        }
      }
    }
    return ALLUXIO_ETCD_CLIENT.get();
  }

  public void connect() {
    connect(false);
  }

  public void connect(boolean force) {
    if (mConnected.get() && !force) {
      return;
    }
    mConnected.set(false);
    // create client using endpoints
    Client client = Client.builder().endpoints(mEndpoints)
        .build();
    if (mConnected.compareAndSet(false, true)) {
      mClient = client;
    }
  }

  public void disconnect() throws IOException {
    close();
  }

  enum WatchType {
    CHILDREN,
    SINGLE_PATH
  }

  public class Lease {
    public long mLeaseId = -1;
    public long mTtlInSec = -1;
    public Lease(long leaseId, long ttlInSec) {
      mLeaseId = leaseId;
      mTtlInSec = ttlInSec;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("leaseId", mLeaseId)
          .add("ttl", mTtlInSec)
          .toString();
    }
  }

  public static final long sDefaultLeaseTTLInSec = 2L;
  public static final long sDefaultTimeoutInSec = 2L;
  public static final int RETRY_TIMES = 3;
  private static final int RETRY_SLEEP_IN_MS = 100;
  private static final int MAX_RETRY_SLEEP_IN_MS = 500;

  public Lease createLease(long ttlInSec, long timeout, TimeUnit timeUnit) {
    return RetryUtils.retryCallable(String.format("Creating Lease ttl:{}", ttlInSec), () -> {
      CompletableFuture<LeaseGrantResponse> leaseGrantFut =
          getEtcdClient().getLeaseClient().grant(ttlInSec, timeout, timeUnit);
      long leaseId;
      LeaseGrantResponse resp = leaseGrantFut.get();
      leaseId = resp.getID();
      Lease lease = new Lease(leaseId, ttlInSec);
      return lease;
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  public Lease createLease() {
    return createLease(sDefaultLeaseTTLInSec, sDefaultTimeoutInSec, TimeUnit.SECONDS);
  }

  public void revokeLease(Lease lease) {
    RetryUtils.retryCallable(String.format("Revoking Lease:{}", lease), () -> {
      CompletableFuture<LeaseRevokeResponse> leaseRevokeFut =
          getEtcdClient().getLeaseClient().revoke(lease.mLeaseId);
      long leaseId;
      LeaseRevokeResponse resp = leaseRevokeFut.get();
      return null;
    }, new ExponentialBackoffRetry(100, 500, RETRY_TIMES));
  }

  public void addChildren(String parentPath, String childPath, byte[] value) {
    Preconditions.checkState(!StringUtil.isNullOrEmpty(parentPath));
    Preconditions.checkState(!StringUtil.isNullOrEmpty(childPath));
    RetryUtils.retryCallable(
        String.format("Adding child, parentPath:{}, childPath:{}",parentPath, childPath),
        () -> {
          String fullPath = parentPath + childPath;
          PutResponse putResponse = mClient.getKVClient().put(ByteSequence.from(fullPath, StandardCharsets.UTF_8),
                  ByteSequence.from(value))
              .get(sDefaultTimeoutInSec, TimeUnit.SECONDS);
          return true;
        },
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, 0));
  }

  public List<KeyValue> getChildren(String parentPath) {
    return RetryUtils.retryCallable(String.format("Getting children for path:{}", parentPath), () -> {
      Preconditions.checkState(!StringUtil.isNullOrEmpty(parentPath));
      GetResponse getResponse = mClient.getKVClient().get(ByteSequence.from(parentPath, StandardCharsets.UTF_8),
          GetOption.newBuilder().isPrefix(true).build())
          .get(sDefaultTimeoutInSec, TimeUnit.SECONDS);
      return getResponse.getKvs();
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  private void addListenerInternal(
      String parentPath, StateListener listener, WatchType watchType) {
    if (mRegisteredWatchers.containsKey(getRegisterWatcherKey(parentPath, watchType))) {
      LOG.info("Watcher already there for path:{} for children.", parentPath);
      return;
    }
    WatchOption.Builder watchOptBuilder = WatchOption.newBuilder();
    switch (watchType) {
      case CHILDREN:
        String keyRangeEnd = parentPath.substring(0, parentPath.length() - 1)
            + (char)(parentPath.charAt(parentPath.length() - 1) + 1);
        watchOptBuilder.isPrefix(true)
            .withRange(ByteSequence.from(keyRangeEnd, StandardCharsets.UTF_8));
        break;
      case SINGLE_PATH:
      default:
        break;
    }

    Watch.Watcher watcher = mClient.getWatchClient().watch(
        ByteSequence.from(parentPath, StandardCharsets.UTF_8),
        watchOptBuilder.build(),
        new Watch.Listener() {
          @Override
          public void onNext(WatchResponse response) {
            for (WatchEvent event : response.getEvents()) {
              switch (event.getEventType()) {
                case PUT:
                  listener.onNewPut(event.getKeyValue().getKey().toString(StandardCharsets.UTF_8)
                      , event.getKeyValue().getValue().getBytes());
                  break;
                case DELETE:
                  listener.onNewDelete(event.getKeyValue().getKey().toString(StandardCharsets.UTF_8));
                  break;
                case UNRECOGNIZED:
                default:
                  LOG.info("Unrecognized event on watch path of:{}", parentPath);
                  break;
              }
            }
          }

          @Override
          public void onError(Throwable throwable) {
            LOG.warn("Error occurred on children watch for path:{}, removing the watch.",
                parentPath, throwable);
            removeChildrenListener(parentPath);
          }

          @Override
          public void onCompleted() {
            LOG.warn("Watch for path onCompleted:{}, removing the watch.", parentPath);
            removeChildrenListener(parentPath);
          }
        });
    Watch.Watcher prevWatcher = mRegisteredWatchers.putIfAbsent(
        getRegisterWatcherKey(parentPath, watchType), watcher);
    // another same watcher already added in a race, close current one
    if (prevWatcher != null) {
      watcher.close();
    } else {
      mCloser.register(watcher);
    }
  }

  private String getRegisterWatcherKey(String path, WatchType type) {
    return path + "$$@@$$" + type.toString();
  }

  public void addStateListener(String path, StateListener listener) {
    addListenerInternal(path, listener, WatchType.SINGLE_PATH);
  }

  public void addChildrenListener(String parentPath, StateListener listener) {
    addListenerInternal(parentPath, listener, WatchType.CHILDREN);
  }

  public void removeChildrenListener(String parentPath) {
    removeListenerInternal(parentPath, WatchType.CHILDREN);
  }

  public void removeStateListener(String path) {
    removeListenerInternal(path, WatchType.SINGLE_PATH);
  }

  // get latest value attached to the key
  public byte[] getForPath(String path) throws IOException {
    return RetryUtils.retryCallable(String.format("Get for path:{}", path), () -> {
      byte[] ret = null;
      try {
        CompletableFuture<GetResponse> getResponse =
            getEtcdClient().getKVClient().get(ByteSequence.from(path, StandardCharsets.UTF_8));
        List<KeyValue> kvs = getResponse.get(sDefaultTimeoutInSec, TimeUnit.SECONDS).getKvs();
        if (!kvs.isEmpty()) {
          KeyValue latestKv = Collections.max(kvs, Comparator.comparing(KeyValue::getModRevision));
          return latestKv.getValue().getBytes();
        }
      } catch (ExecutionException | InterruptedException ex) {
        throw new IOException("Error getting path:" + path, ex);
      }
      return ret;
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  public boolean checkExistsForPath(String path) {
    return RetryUtils.retryCallable(String.format("Get for path:{}", path), () -> {
      boolean exist = false;
      try {
        CompletableFuture<GetResponse> getResponse =
            getEtcdClient().getKVClient().get(ByteSequence.from(path, StandardCharsets.UTF_8));
        List<KeyValue> kvs = getResponse.get(sDefaultTimeoutInSec, TimeUnit.SECONDS).getKvs();
        exist = !kvs.isEmpty();
      } catch (ExecutionException | InterruptedException ex) {
        throw new IOException("Error getting path:" + path, ex);
      }
      return exist;
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, 0));
  }

  public void createForPath(String path, Optional<byte[]> value) throws IOException {
    RetryUtils.retryCallable(String.format("Get for path:{}, value size:{}",
        path, (value.isEmpty() ? "null" : value.get().length)), () -> {
      try {
        mClient.getKVClient().put(ByteSequence.from(path, StandardCharsets.UTF_8)
                , ByteSequence.from(value.get()))
            .get();
      } catch (ExecutionException | InterruptedException ex) {
        throw new IOException("Error getting path:" + path, ex);
      }
      return null;
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  public void deleteForPath(String path) {
    RetryUtils.retryCallable(String.format("Delete for path:{}", path), () -> {
      try {
        mClient.getKVClient().delete(ByteSequence.from(path, StandardCharsets.UTF_8))
            .get();
      } catch (ExecutionException | InterruptedException ex) {
        throw new IOException("Error deleting path:" + path, ex);
      }
      return null;
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  public void removeListenerInternal(String path, WatchType watchType) {
    Watch.Watcher watcher = mRegisteredWatchers.remove(getRegisterWatcherKey(path, watchType));
    if (watcher == null) {
      return;
    }
    watcher.close();
  }

  public boolean isConnected() {
    return mConnected.get();
  }

  public Client getEtcdClient() {
    if (mConnected.get()) {
      return mClient;
    }
    connect();
    return mClient;
  }

  @Override
  public void close() throws IOException {
    if (mClient != null) {
      mClient.close();
    }
    mCloser.close();
  }

  public static void testBarrier(AlluxioEtcdClient alluxioEtcdClient) {
    try {
      BarrierRecipe barrierRecipe = new BarrierRecipe(alluxioEtcdClient, "/barrier-test",
          "cluster1", 2L);
      LOG.info("Setting barrier.");
      barrierRecipe.setBarrier();
      Thread t = new Thread(() -> {
        try {
          LOG.info("start waiting on barrier...");
          barrierRecipe.waitOnBarrier();
          LOG.info("wait on barrier done.");
        } catch (InterruptedException e) {
          LOG.info("wait on barrier ex:", e);
          throw new RuntimeException(e);
        }
      });
      t.start();
      Thread.sleep(3000);
      LOG.info("Removing barrier.");
      barrierRecipe.removeBarrier();
      t.join();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void main(String[] args) {
    BasicConfigurator.configure();
    AlluxioEtcdClient alluxioEtcdClient = new AlluxioEtcdClient(Configuration.global());
    alluxioEtcdClient.connect();
//    testServiceDiscovery(etcdClient);
//    testBarrier(etcdClient);

    try {
//      etcdClient.mClient.getWatchClient().watch(ByteSequence.from("/lucy1", StandardCharsets.UTF_8),
//          WatchOption.newBuilder().withRevision(70L).build(), watchResponse -> {
//            for (WatchEvent event : watchResponse.getEvents()) {
//              if (event.getEventType() == WatchEvent.EventType.PUT) {
//                LOG.info("PUT event observed on path {}, createrevision:{}, modifyrevision:{}, version:{}",
//                    "/lucy1", event.getKeyValue().getCreateRevision(), event.getKeyValue().getModRevision()
//                    , event.getKeyValue().getVersion());
//              }
//            }
//          });
//      GetResponse resp = etcdClient.mClient.getKVClient()
//          .get(ByteSequence.from("/lucy", StandardCharsets.UTF_8)).get();
//      for (KeyValue kv : resp.getKvs()) {
//        LOG.info("[LUCY]k:{}:v:{}:version:{}:createVersion:{}:modifyVersion:{}:lease:{}",
//            kv.getKey().toString(StandardCharsets.UTF_8), kv.getValue().toString(StandardCharsets.UTF_8),
//            kv.getVersion(), kv.getCreateRevision(), kv.getModRevision(), kv.getLease());
//      }
      String fullPath = "/lucytest0612";
      Txn txn = alluxioEtcdClient.mClient.getKVClient().txn();
      ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
      ByteSequence valToPut = ByteSequence.from("abc", StandardCharsets.UTF_8);
      CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.modRevision(78L)))
          .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().build()))
          .Then(Op.get(keyToPut, GetOption.DEFAULT))
          .Else(Op.get(keyToPut, GetOption.DEFAULT))
          .commit();
      TxnResponse resp = txnResponseFut.get();
      LOG.info("resp.isSucceeded:{}", resp.isSucceeded());
      List<KeyValue> kvs = new ArrayList<>();
      resp.getGetResponses().stream().map(r -> kvs.addAll(r.getKvs())).collect(Collectors.toList());
      List<String> outputs = kvs.stream().map(kv -> kv.getKey().toString(StandardCharsets.UTF_8) + ":"
          + kv.getValue().toString(StandardCharsets.UTF_8) + "[" + kv.getModRevision() + "]").collect(Collectors.toList());
      LOG.info("resp kv:{}", outputs);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
    LOG.info("[LUCY] main done.");
  }

//  private static void init() {
//    PropertyConfigurator.configure("/Users/lucyge/Documents/github/alluxio/conf/log4j.properties");
//    Properties props = new Properties();
//    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
//  }
}
