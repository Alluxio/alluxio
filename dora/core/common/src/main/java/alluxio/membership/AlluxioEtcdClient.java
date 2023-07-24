/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.resource.LockResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * Wrapper class around jetcd client to achieve utilities API to talk with ETCD.
 */
public class AlluxioEtcdClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEtcdClient.class);
  private static final Lock INSTANCE_LOCK = new ReentrantLock();
  @GuardedBy("INSTANCE_LOCK")
  private static final AtomicReference<AlluxioEtcdClient> ALLUXIO_ETCD_CLIENT
      = new AtomicReference<>();
  protected AtomicBoolean mConnected = new AtomicBoolean(false);
  private Client mClient;
  public final ServiceDiscoveryRecipe mServiceDiscovery;
  public String[] mEndpoints;
  private final Closer mCloser = Closer.create();
  // only watch for children change(add/remove) for given parent path
  private final ConcurrentHashMap<String, Watch.Watcher> mRegisteredWatchers =
      new ConcurrentHashMap<>();

  /**
   * CTOR for AlluxioEtcdClient.
   * @param conf
   */
  public AlluxioEtcdClient(AlluxioConfiguration conf) {
    String clusterName = conf.getString(PropertyKey.ALLUXIO_CLUSTER_NAME);
    List<String> endpointsList = conf.getList(PropertyKey.ETCD_ENDPOINTS);
    mEndpoints = endpointsList.toArray(new String[endpointsList.size()]);
    mServiceDiscovery = new ServiceDiscoveryRecipe(this, clusterName);
  }

  /**
   * Get the singleton instance of AlluxioEtcdClient.
   * @param conf
   * @return AlluxioEtcdClient
   */
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

  /**
   * Create jetcd grpc client no forcing.
   */
  public void connect() {
    connect(false);
  }

  /**
   * Create jetcd grpc client with choice of force or not.
   * @param force
   */
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

  /**
   * Disconnect.
   * @throws IOException
   */
  public void disconnect() throws IOException {
    close();
  }

  /**
   * Watch for a single path or the change among all children of this path.
   */
  enum WatchType {
    CHILDREN,
    SINGLE_PATH
  }

  /**
   * Lease structure to keep the info about a lease in etcd.
   */
  public static class Lease {
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

  public static final long DEFAULT_LEASE_TTL_IN_SEC = 2L;
  public static final long DEFAULT_TIMEOUT_IN_SEC = 2L;
  public static final int RETRY_TIMES = 3;
  private static final int RETRY_SLEEP_IN_MS = 100;
  private static final int MAX_RETRY_SLEEP_IN_MS = 500;

  /**
   * Create a lease with timeout and ttl.
   * @param ttlInSec
   * @param timeout
   * @param timeUnit
   * @return Lease
   * @throws IOException
   */
  public Lease createLease(long ttlInSec, long timeout, TimeUnit timeUnit)
      throws IOException {
    try {
      return RetryUtils.retryCallable(String.format("Creating Lease ttl:%s", ttlInSec), () -> {
        CompletableFuture<LeaseGrantResponse> leaseGrantFut =
            getEtcdClient().getLeaseClient().grant(ttlInSec, timeout, timeUnit);
        long leaseId;
        LeaseGrantResponse resp = leaseGrantFut.get(timeout, timeUnit);
        leaseId = resp.getID();
        Lease lease = new Lease(leaseId, ttlInSec);
        return lease;
      }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
    } catch (AlluxioRuntimeException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Create lease with default ttl and timeout.
   * @return Lease
   * @throws IOException
   */
  public Lease createLease() throws IOException {
    return createLease(DEFAULT_LEASE_TTL_IN_SEC, DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
  }

  /**
   * Revoke given lease.
   * @param lease
   * @throws IOException
   */
  public void revokeLease(Lease lease) throws IOException {
    RetryUtils.retry(String.format("Revoking Lease:%s", lease.toString()), () -> {
      try {
        CompletableFuture<LeaseRevokeResponse> leaseRevokeFut =
            getEtcdClient().getLeaseClient().revoke(lease.mLeaseId);
        long leaseId;
        LeaseRevokeResponse resp = leaseRevokeFut.get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
      } catch (ExecutionException | InterruptedException | TimeoutException ex) {
        throw new IOException("Error revoking lease:" + lease.toString(), ex);
      }
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  /**
   * Check with etcd if a lease is already expired.
   * @param lease
   * @return lease expired
   */
  public boolean isLeaseExpired(Lease lease) throws IOException {
    try {
      return RetryUtils.retryCallable(
          String.format("Checking IsLeaseExpired, lease:%s", lease.toString()),
          () -> {
            LeaseTimeToLiveResponse leaseResp = mClient.getLeaseClient()
                .timeToLive(lease.mLeaseId, LeaseOption.DEFAULT)
                .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
            // if no such lease, lease resp will still be returned with a negative ttl
            return leaseResp.getTTl() <= 0;
          }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
    } catch (AlluxioRuntimeException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Create a childPath with value to a parentPath.
   * e.g. create "lower_path" under path /upper_path/ to form a
   * kv pair of /upper_path/lower_path with a given value.
   * @param parentPath
   * @param childPath
   * @param value
   */
  public void addChildren(String parentPath, String childPath, byte[] value)
      throws IOException {
    Preconditions.checkArgument(!StringUtil.isNullOrEmpty(parentPath));
    Preconditions.checkArgument(!StringUtil.isNullOrEmpty(childPath));
    RetryUtils.retry(
        String.format("Adding child, parentPath:%s, childPath:%s", parentPath, childPath),
        () -> {
          try {
            String fullPath = parentPath + childPath;
            PutResponse putResponse = mClient.getKVClient().put(
                    ByteSequence.from(fullPath, StandardCharsets.UTF_8),
                    ByteSequence.from(value))
                .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            String errMsg = String.format("Error addChildren parentPath:%s child:%s",
                parentPath, childPath);
            throw new IOException(errMsg, ex);
          }
        },
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, 0));
  }

  /**
   * Get list of children path kv pairs from a given parentPath
   * e.g. get [/upper/lower1 - val1, /upper/lower2 - val2]
   * under parent path /upper/
   * @param parentPath parentPath ends with /
   * @return list of children KeyValues.
   */
  public List<KeyValue> getChildren(String parentPath) throws IOException {
    try {
      return RetryUtils.retryCallable(String.format("Getting children for path:%s", parentPath),
          () -> {
            Preconditions.checkState(!StringUtil.isNullOrEmpty(parentPath));
            GetResponse getResponse = mClient.getKVClient().get(
                    ByteSequence.from(parentPath, StandardCharsets.UTF_8),
                    GetOption.newBuilder().isPrefix(true).build())
                .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
            return getResponse.getKvs();
          }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
    } catch (AlluxioRuntimeException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Add listener to a path internal function.
   * @param parentPath
   * @param listener
   * @param watchType
   */
  private void addListenerInternal(
      String parentPath, StateListener listener, WatchType watchType) {
    if (mRegisteredWatchers.containsKey(getRegisterWatcherKey(parentPath, watchType))) {
      LOG.info("Watcher already there for path:{} for children.", parentPath);
      return;
    }
    WatchOption.Builder watchOptBuilder = WatchOption.newBuilder();
    switch (watchType) {
      /* e.g. Given the parentPath '/parent/',
      give query-like syntax equivalent to:
        select * with value < '/parent.' ('.' the char before '/' in ASCII)
      which includes all keys prefixed with '/parent/' */
      case CHILDREN:
        String keyRangeEnd = parentPath.substring(0, parentPath.length() - 1)
            + (char) (parentPath.charAt(parentPath.length() - 1) + 1);
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
                  listener.onNewPut(
                      event.getKeyValue().getKey().toString(StandardCharsets.UTF_8),
                      event.getKeyValue().getValue().getBytes());
                  break;
                case DELETE:
                  listener.onNewDelete(
                      event.getKeyValue().getKey().toString(StandardCharsets.UTF_8));
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

  /**
   * Get the registered watch key in the map.
   * @param path
   * @param type
   * @return key for registered watcher
   */
  private String getRegisterWatcherKey(String path, WatchType type) {
    return path + "$$@@$$" + type.toString();
  }

  /**
   * Add state listener to given path.
   * @param path
   * @param listener
   */
  public void addStateListener(String path, StateListener listener) {
    addListenerInternal(path, listener, WatchType.SINGLE_PATH);
  }

  /**
   * Remove state listener for give path.
   * @param path
   */
  public void removeStateListener(String path) {
    removeListenerInternal(path, WatchType.SINGLE_PATH);
  }

  /**
   * Add state listener to watch children for given path.
   * @param parentPath
   * @param listener
   */
  public void addChildrenListener(String parentPath, StateListener listener) {
    addListenerInternal(parentPath, listener, WatchType.CHILDREN);
  }

  /**
   * Remove state listener for children on a given parentPath.
   * @param parentPath
   */
  public void removeChildrenListener(String parentPath) {
    removeListenerInternal(parentPath, WatchType.CHILDREN);
  }

  /**
   * Get latest value attached to the key.
   * @param path
   * @return
   * @throws IOException
   */
  public byte[] getForPath(String path) throws IOException {
    try {
      return RetryUtils.retryCallable(String.format("Get for path:%s", path),
          () -> {
            byte[] ret = null;
            CompletableFuture<GetResponse> getResponse =
                getEtcdClient().getKVClient().get(ByteSequence.from(path, StandardCharsets.UTF_8));
            List<KeyValue> kvs = getResponse.get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS).getKvs();
            if (!kvs.isEmpty()) {
              KeyValue latestKv = Collections.max(
                  kvs, Comparator.comparing(KeyValue::getModRevision));
              return latestKv.getValue().getBytes();
            }
            return ret;
          }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
    } catch (AlluxioRuntimeException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Check existence of a given path.
   * @param path
   * @return if the path exists or not
   * @throws IOException
   */
  public boolean checkExistsForPath(String path) throws IOException {
    try {
      return RetryUtils.retryCallable(String.format("Get for path:%s", path),
          () -> {
            boolean exist = false;
            try {
              CompletableFuture<GetResponse> getResponse =
                  getEtcdClient().getKVClient().get(ByteSequence.from(path, StandardCharsets.UTF_8));
              List<KeyValue> kvs = getResponse.get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS).getKvs();
              exist = !kvs.isEmpty();
            } catch (ExecutionException | InterruptedException | TimeoutException ex) {
              throw new IOException("Error getting path:" + path, ex);
            }
            return exist;
          }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
    } catch (AlluxioRuntimeException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Create a path with given value in non-transactional way.
   * @param path
   * @param value
   * @throws IOException
   */
  public void createForPath(String path, Optional<byte[]> value) throws IOException {
    RetryUtils.retry(String.format("Get for path:%s, value size:%s",
        path, (!value.isPresent() ? "null" : value.get().length)),
        () -> {
          try {
            mClient.getKVClient().put(
                    ByteSequence.from(path, StandardCharsets.UTF_8)
                    , ByteSequence.from(value.get()))
                .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            String errMsg = String.format("Error createForPath:%s", path);
            throw new IOException(errMsg, ex);
          }
    }, new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES));
  }

  /**
   * Delete a path or recursively all paths with given path as prefix.
   * @param path
   * @param recursive
   * @throws IOException
   */
  public void deleteForPath(String path, boolean recursive) throws IOException {
    RetryUtils.retry(String.format("Delete for path:%s", path),
        () -> {
          try {
            mClient.getKVClient().delete(
                    ByteSequence.from(path, StandardCharsets.UTF_8)
                    , DeleteOption.newBuilder().isPrefix(recursive).build())
                .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            String errMsg = String.format("Error deleteForPath:%s", path);
            throw new IOException(errMsg, ex);
          }
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
}
