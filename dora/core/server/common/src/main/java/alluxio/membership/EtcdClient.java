package alluxio.membership;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.UnavailableException;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import alluxio.wire.WorkerNetAddress;
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
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.stub.StreamObserver;
import io.netty.util.internal.StringUtil;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class EtcdClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdClient.class);

  protected AtomicBoolean mConnected = new AtomicBoolean(false);
  private Client mEtcdClient;
  public final ServiceDiscoveryRecipe mServiceDiscovery;
  public List<URI> mEndpoints = new ArrayList<>();
  private final Closer mCloser = Closer.create();

  public EtcdClient(String cluserName) {
    mServiceDiscovery = new ServiceDiscoveryRecipe(this, cluserName, 2L);
  }

  public EtcdClient(String cluserName, List<URI> endpoints) {
    mEndpoints.addAll(endpoints);
    mServiceDiscovery = new ServiceDiscoveryRecipe(this, cluserName, 2L);
  }

  public static void getInstance() {

  }

  public void connect() {
    if (mConnected.get()) {
      return;
    }
    List<String> endpoints = new ArrayList<>();

    // create client using endpoints
    Client client = Client.builder().endpoints(mEndpoints)
//        .endpoints(
//        "http://localhost:2379" //, "http://etcd1:2379", "http://etcd2:2379"
//        )
        .build();
    if (mConnected.compareAndSet(false, true)) {
      mEtcdClient = client;
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
          PutResponse putResponse = mEtcdClient.getKVClient().put(ByteSequence.from(fullPath, StandardCharsets.UTF_8),
                  ByteSequence.from(value))
              .get(sDefaultTimeoutInSec, TimeUnit.SECONDS);
          return true;
        },
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, 0));
  }

  public List<KeyValue> getChildren(String parentPath) {
    return RetryUtils.retryCallable(String.format("Getting children for path:{}", parentPath), () -> {
      Preconditions.checkState(!StringUtil.isNullOrEmpty(parentPath));
      GetResponse getResponse = mEtcdClient.getKVClient().get(ByteSequence.from(parentPath, StandardCharsets.UTF_8),
          GetOption.newBuilder().isPrefix(true).build())
          .get(sDefaultTimeoutInSec, TimeUnit.SECONDS);
      return getResponse.getKvs();
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  // only watch for children change(add/remove) for given parent path
  private ConcurrentHashMap<String, Watch.Watcher> mRegisteredWatchers =
      new ConcurrentHashMap<>();

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

    Watch.Watcher watcher = mEtcdClient.getWatchClient().watch(
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
        mEtcdClient.getKVClient().put(ByteSequence.from(path, StandardCharsets.UTF_8)
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
        mEtcdClient.getKVClient().delete(ByteSequence.from(path, StandardCharsets.UTF_8))
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
      return mEtcdClient;
    }
    connect();
    return mEtcdClient;
  }

  @Override
  public void close() throws IOException {
    if (mEtcdClient != null) {
      mEtcdClient.close();
    }
    mCloser.close();
  }

  public static class TestService extends EtcdClient.ServiceEntityContext {
    AtomicReference<Long> mWorkerId;
    WorkerNetAddress mAddress;
    Long mLeaseId = -1L;

    public TestService(String id) {
      super(id);
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("WorkerId", mWorkerId.get())
//          .add("WorkerAddr", mAddress.toString())
          .add("LeaseId", mLeaseId)
          .toString();
    }
  }

  public static void testServiceDiscovery(EtcdClient etcdClient) {
    try {
      String clusterId = UUID.randomUUID().toString();
      ServiceDiscoveryRecipe sd = new ServiceDiscoveryRecipe(etcdClient,
          clusterId, 2L);
      TestService service = new TestService("worker-0");
//      service.mAddress = new WorkerNetAddress()
//          .setHost(NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC,
//              Configuration.global()))
//          .setContainerHost(Configuration.global()
//              .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
//          .setRpcPort(1234)
//          .setDataPort(2234)
//          .setWebPort(3344);
      service.mWorkerId = new AtomicReference<Long>(12L);
      System.out.println("registering  service," + service);
      sd.registerAndStartSync(service);
      sd.getAllLiveServices();
      Thread.sleep(30000);
      System.out.println("unregistering  service," + service);
      sd.unregisterService(service.getServiceEntityName());
      System.out.println("finished main.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void testBarrier(EtcdClient etcdClient) {
    try {
      BarrierRecipe barrierRecipe = new BarrierRecipe(etcdClient, "/barrier-test",
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
    EtcdClient etcdClient = new EtcdClient("Default");
    etcdClient.connect();
//    testServiceDiscovery(etcdClient);
//    testBarrier(etcdClient);

    try {
//      etcdClient.mEtcdClient.getWatchClient().watch(ByteSequence.from("/lucy1", StandardCharsets.UTF_8),
//          WatchOption.newBuilder().withRevision(70L).build(), watchResponse -> {
//            for (WatchEvent event : watchResponse.getEvents()) {
//              if (event.getEventType() == WatchEvent.EventType.PUT) {
//                LOG.info("PUT event observed on path {}, createrevision:{}, modifyrevision:{}, version:{}",
//                    "/lucy1", event.getKeyValue().getCreateRevision(), event.getKeyValue().getModRevision()
//                    , event.getKeyValue().getVersion());
//              }
//            }
//          });
//      GetResponse resp = etcdClient.mEtcdClient.getKVClient()
//          .get(ByteSequence.from("/lucy", StandardCharsets.UTF_8)).get();
//      for (KeyValue kv : resp.getKvs()) {
//        LOG.info("[LUCY]k:{}:v:{}:version:{}:createVersion:{}:modifyVersion:{}:lease:{}",
//            kv.getKey().toString(StandardCharsets.UTF_8), kv.getValue().toString(StandardCharsets.UTF_8),
//            kv.getVersion(), kv.getCreateRevision(), kv.getModRevision(), kv.getLease());
//      }
      String fullPath = "/lucytest0612";
      Txn txn = etcdClient.mEtcdClient.getKVClient().txn();
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

  private static void init() {
    PropertyConfigurator.configure("/Users/lucyge/Documents/github/alluxio/conf/log4j.properties");
    Properties props = new Properties();
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
  }

  public static class ServiceEntityContext implements Closeable {
    private CloseableClient mKeepAliveClient;
    private Client mEtcdClient;
    Lease mLease; // used for keep alive(heartbeating) will not be set on start up
    private String mServiceEntityName; // user defined name for this service entity (e.g. worker-0)
    protected long mRevision;

    public ServiceEntityContext() {

    }
    public ServiceEntityContext(String serviceEntityName) {
      mServiceEntityName = serviceEntityName;
    }

    public String getServiceEntityName() {
      return mServiceEntityName;
    }

    @Override
    public void close() throws IOException {
      if (mKeepAliveClient != null) {
        mKeepAliveClient.close();
      }
    }

    public void serialize(DataOutput out) throws IOException {
      out.writeUTF(mServiceEntityName);
      out.writeLong(mRevision);
    }

    public void deserialize(DataInput in) throws IOException {
      mServiceEntityName = in.readUTF();
      mRevision = in.readLong();
    }
  }

  public static class ServiceDiscoveryRecipe {
    String basePath = "/ServiceDiscovery";
    Client mClient;
    EtcdClient mEtcdClient;
    String mClusterIdentifier;
    final long mLeaseTtlInSec;
    private final ReentrantLock mRegisterLock = new ReentrantLock();
    final ConcurrentHashMap<String, ServiceEntityContext> mRegisteredServices = new ConcurrentHashMap<>();
    public ServiceDiscoveryRecipe(EtcdClient client, String clusterIdentifier, long leaseTtlSec) {
      mEtcdClient = client;
      mEtcdClient.connect();
      mClient = client.getEtcdClient();
      mClusterIdentifier = clusterIdentifier;
      mLeaseTtlInSec = leaseTtlSec;
    }

    @GuardedBy("ServiceDiscoveryRecipe#mRegisterLock")
    public void registerAndStartSync(ServiceEntityContext service) throws IOException {
      LOG.info("registering service : {}", service);
      if (mRegisteredServices.containsKey(service.mServiceEntityName)) {
        throw new AlreadyExistsException("Service " + service.mServiceEntityName + " already registerd.");
      }
      String path = service.mServiceEntityName;
      String fullPath = basePath + "/" + mClusterIdentifier + "/" + path;
      try {
        Lease lease = mEtcdClient.createLease();
        Txn txn = mClient.getKVClient().txn();
        ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        service.serialize(dos);
        ByteSequence valToPut = ByteSequence.from(baos.toByteArray());
        CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.version(0L)))
            .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().withLeaseId(lease.mLeaseId).build()))
            .Then(Op.get(keyToPut, GetOption.DEFAULT))
            .Else(Op.get(keyToPut, GetOption.DEFAULT))
            .commit();
        TxnResponse txnResponse = txnResponseFut.get();
        List<KeyValue> kvs = new ArrayList<>();
        txnResponse.getGetResponses().stream().map(
            r -> kvs.addAll(r.getKvs())).collect(Collectors.toList());
        if (!txnResponse.isSucceeded()) {
          // Already authorized
          if (!kvs.isEmpty()) {
            throw new AlreadyExistsException("Some process already registered same service and syncing,"
                + "this should not happen");
          }
          throw new IOException("Failed to register service:" + service.toString());
//          KeyValue kv = Collections.max(kvs, Comparator.comparing(KeyValue::getModRevision));
//          ByteArrayOutputStream baos = new ByteArrayOutputStream();
//          DataOutputStream dos = new DataOutputStream(baos);
//          service.serialize(dos);
//          byte[] serializedBytes = baos.toByteArray();
//          ByteSequence val = ByteSequence.from(serializedBytes);
//          if (val.equals(kv.getValue())) {
//            LOG.info("Same service already registered, start sync.");
//          }
        }
        Preconditions.checkState(!kvs.isEmpty(), "No such service entry found.");
        long latestRevision = kvs.stream().mapToLong(kv -> kv.getModRevision()).max().getAsLong();
        service.mRevision = latestRevision;
        service.mLease = lease;
        service.mKeepAliveClient = mClient.getLeaseClient()
            .keepAlive(service.mLease.mLeaseId, mKeepAliveObserver);
        mRegisteredServices.put(service.mServiceEntityName, service);
      } catch (ExecutionException ex) {
        throw new IOException("ExecutionException in registering service:" + service, ex);
      } catch (InterruptedException ex) {
        LOG.info("InterruptedException caught, bail.");
      }
    }

    @GuardedBy("ServiceDiscoveryRecipe#mRegisterLock")
    public void unregisterService(String serviceIdentifier) throws IOException {
      if (!mRegisteredServices.containsKey(serviceIdentifier)) {
        LOG.info("Service {} already unregistered.", serviceIdentifier);
        return;
      }
      try (ServiceEntityContext service = mRegisteredServices.get(serviceIdentifier)) {
        boolean removed = mRegisteredServices.remove(serviceIdentifier, service);
        LOG.info("Unregister service {} : {}", service, (removed) ? "success" : "failed");
      }
    }

    public void getRegisteredServiceDetail(String serviceEntityName, ServiceEntityContext ctx)
        throws IOException {
      String fullPath = basePath + "/" + mClusterIdentifier + "/" + serviceEntityName;
      byte[] val = mEtcdClient.getForPath(fullPath);
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val));
      ctx.deserialize(dis);
    }

    public void updateService(ServiceEntityContext service) throws IOException {
      LOG.info("Updating service : {}", service);
      if (!mRegisteredServices.containsKey(service.mServiceEntityName)) {
        Preconditions.checkNotNull(service.mLease, "Service not attach with lease");
        throw new NoSuchElementException("Service " + service.mServiceEntityName
            + " not registered, please register first.");
      }
      String path = service.mServiceEntityName;
      String fullPath = basePath + "/" + mClusterIdentifier + "/" + path;
      try {
        Txn txn = mClient.getKVClient().txn();
        ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
        ByteSequence valToPut = ByteSequence.from(service.toString(), StandardCharsets.UTF_8);
        CompletableFuture<TxnResponse> txnResponseFut = txn
            .If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.modRevision(service.mRevision)))
            .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().withLeaseId(service.mLease.mLeaseId).build()))
            .Then(Op.get(keyToPut, GetOption.DEFAULT))
            .commit();
        TxnResponse txnResponse = txnResponseFut.get();
        // return if Cmp returns true
        if (!txnResponse.isSucceeded()) {
          throw new IOException("Failed to update service:" + service.toString());
        }
        service.mKeepAliveClient = mClient.getLeaseClient()
            .keepAlive(service.mLease.mLeaseId, mKeepAliveObserver);
        mRegisteredServices.put(service.mServiceEntityName, service);
      } catch (ExecutionException ex) {
        throw new IOException("ExecutionException in registering service:" + service, ex);
      } catch (InterruptedException ex) {
        LOG.info("InterruptedException caught, bail.");
      }
    }

    StreamObserver<LeaseKeepAliveResponse> mKeepAliveObserver = new StreamObserver<LeaseKeepAliveResponse>() {
      @Override
      public void onNext(LeaseKeepAliveResponse value) {
        LOG.info("onNext:id:{}:ttl:{}", value.getID(), value.getTTL());
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("onError:{}", t);
      }

      @Override
      public void onCompleted() {
        LOG.info("onCompleted");
      }
    };

    public Map<String, ByteBuffer> getAllLiveServices() {
      String clusterPath = basePath + "/" + mClusterIdentifier;
      Map<String, ByteBuffer> ret = new HashMap<>();
      List<KeyValue> children = mEtcdClient.getChildren(clusterPath);
      for (KeyValue kv : children) {
        ret.put(kv.getKey().toString(StandardCharsets.UTF_8),
            ByteBuffer.wrap(kv.getValue().getBytes()));
      }

        return ret;
//        GetResponse getResponse = mClient.getKVClient()
//            .get(ByteSequence.from(clusterPath, StandardCharsets.UTF_8),
//                GetOption.newBuilder().isPrefix(true).build())
//            .get();
//        List<KeyValue> kvs = getResponse.getKvs();
//        LOG.info("[LUCY]:kvs:path:{}", clusterPath);
//        for (KeyValue kv : kvs) {
//          LOG.info("[LUCY]k:{}:v:{}:version:{}:createVersion:{}:modifyVersion:{}:lease:{}",
//              kv.getKey().toString(StandardCharsets.UTF_8), kv.getValue().toString(StandardCharsets.UTF_8),
//              kv.getVersion(), kv.getCreateRevision(), kv.getModRevision(), kv.getLease());
//        }
    }

  }

  public static class BarrierRecipe {
    Client mClient;
    String mClusterIdentifier;
    long mLeaseTtlInSec = 2L;
    String mBarrierPath;
    String mNewBarrierPath = "/new-barrier";
    CountDownLatch mLatch = new CountDownLatch(1);
    public BarrierRecipe(EtcdClient client, String barrierPath, String clusterIdentifier, long leaseTtlSec) {
      client.connect();
      mClient = client.getEtcdClient();
      mClusterIdentifier = clusterIdentifier;
      mLeaseTtlInSec = leaseTtlSec;
      mBarrierPath = barrierPath;
    }

    public void setBarrier() throws IOException {
      try {
        Txn txn = mClient.getKVClient().txn();
        ByteSequence key = ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8);
        CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0L)))
            .Then(Op.put(key, ByteSequence.EMPTY, PutOption.DEFAULT))
            .commit();
        TxnResponse txnResponse = txnResponseFut.get();
        if (!txnResponse.isSucceeded()) {
          throw new IOException("Failed to set barrier for path:" + mBarrierPath);
        }
        LOG.info("Successfully set barrier:{}", mBarrierPath);
      } catch (ExecutionException | InterruptedException ex) {
        LOG.error("Exception during setBarrier.", ex);
      }
    }

    public void removeBarrier() throws IOException {
      try {
        GetResponse getResp = mClient.getKVClient().get(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8)).get();
        LOG.info("get key:{}, [{}]", mBarrierPath, getResp.getKvs());
        Txn txn = mClient.getKVClient().txn();
        ByteSequence key = ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8);
        ByteSequence key1 = ByteSequence.from(mNewBarrierPath, StandardCharsets.UTF_8);
        CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.createRevision(0L)))
            .Then(Op.delete(key, DeleteOption.DEFAULT))
            .Then(Op.put(key1, ByteSequence.EMPTY, PutOption.DEFAULT))
            .commit();
        TxnResponse txnResponse = txnResponseFut.get();
        if (!txnResponse.isSucceeded()) {
          throw new IOException("Failed to remove barrier for path:" + mBarrierPath);
        }
        LOG.info("Successfully remove barrier:{}", mBarrierPath);
      } catch (ExecutionException | InterruptedException ex) {
        LOG.error("Exception during removeBarrier.", ex);
      }
    }

    public void waitOnBarrierInternal() {
      try {
        Watch.Watcher watcher = mClient.getWatchClient().watch(ByteSequence.EMPTY, WatchOption.newBuilder().build(), new Watch.Listener() {
          @Override
          public void onNext(WatchResponse response) {
            WatchEvent event = response.getEvents().get(0);
          }

          @Override
          public void onError(Throwable throwable) {

          }

          @Override
          public void onCompleted() {

          }
        });
      mClient.getWatchClient().watch(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8),
          WatchOption.DEFAULT, watchResponse -> {
            for (WatchEvent event : watchResponse.getEvents()) {
              if (event.getEventType() == WatchEvent.EventType.DELETE &&
                  event.getKeyValue().getKey().equals(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8))) {
                LOG.info("Delete event observed on path {}", mBarrierPath);
                mLatch.countDown();
              }
            }
          });
        mLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      LOG.info("Barrier wait done.");
    }

    // wait forever
    public void waitOnBarrier() throws InterruptedException {
      waitOnBarrierInternal();
      mLatch.await();
    }

    public void waitOnBarrier(long time, TimeUnit timeUnit) throws InterruptedException {
      waitOnBarrierInternal();
      mLatch.await(time, timeUnit);
    }

  }

}
