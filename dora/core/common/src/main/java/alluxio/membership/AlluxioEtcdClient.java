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
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.resource.LockResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Wrapper class around jetcd client to achieve utilities API to talk with ETCD.
 * This class is supposed to be used as a singleton fashion. It wraps around
 * one jetcd Client instance for all sorts of utility functions to interact with etcd.
 * Only state it's keeping is the jetcd Client and registered Watcher list
 * For kv operations such as Put(createForPath, deleteForPath, addChildren, etc.)
 * its atomicity/consistency semantics goes with what ETCD has to offer, this class
 * does not add upon any semantics itself.
 *
 * AlluxioEtcdClient should only be used as singleton wrapping one jetcd Client object,
 * currently only resource - jetcd client will be closed as part of close() which is
 * called during:
 * 1) Worker shutdown or close as part of EtcdMembershipManager close
 * 2) FileSystemContext closeContext as part of EtcdMembershipManager close
 * As we never set mClient to be null after connect, also jetcd client can be closed idempotently
 * so it's ok to ignore thread safety for close()
 *
 * As for jetcd Client, it's managing its own connect/reconnect/loadbalance to other etcd
 * instances, will leave these logic to jetcd client itself for now unless we need to
 * handle it in our layer.
 */
public class AlluxioEtcdClient {

  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEtcdClient.class);
  private static final Lock INSTANCE_LOCK = new ReentrantLock();
  public static final String BASE_PATH = "/ServiceDiscovery";
  public static final long DEFAULT_LEASE_TTL_IN_SEC = 2L;
  public static final long DEFAULT_TIMEOUT_IN_SEC = 2L;
  public static final int RETRY_TIMES = 3;
  private static final int RETRY_SLEEP_IN_MS = 100;
  private static final int MAX_RETRY_SLEEP_IN_MS = 500;
  @GuardedBy("INSTANCE_LOCK")
  @Nullable
  private static volatile AlluxioEtcdClient sAlluxioEtcdClient;
  public final ServiceDiscoveryRecipe mServiceDiscovery;
  // only watch for children change(add/remove) for given parent path
  private final ConcurrentHashMap<String, Watch.Watcher> mRegisteredWatchers =
      new ConcurrentHashMap<>();
  private Client mClient;
  private final String[] mEndpoints;

  /**
   * CTOR for AlluxioEtcdClient.
   * @param conf
   */
  @VisibleForTesting
  public AlluxioEtcdClient(AlluxioConfiguration conf) {
    String clusterName = conf.getString(PropertyKey.ALLUXIO_CLUSTER_NAME);
    List<String> endpointsList = conf.getList(PropertyKey.ETCD_ENDPOINTS);
    mEndpoints = endpointsList.toArray(new String[0]);
    mServiceDiscovery = new ServiceDiscoveryRecipe(this,
        String.format("%s%s%s", BASE_PATH, MembershipManager.PATH_SEPARATOR, clusterName));
    // TODO(lucy) add more options as needed for io.etcd.jetcd.ClientBuilder
    // to control underneath grpc parameters.
    mClient = Client.builder().endpoints(mEndpoints)
        .build();
  }

  /**
   * Get the singleton instance of AlluxioEtcdClient.
   * @param conf
   * @return AlluxioEtcdClient
   */
  public static AlluxioEtcdClient getInstance(AlluxioConfiguration conf) {
    if (sAlluxioEtcdClient == null) {
      try (LockResource lockResource = new LockResource(INSTANCE_LOCK)) {
        if (sAlluxioEtcdClient == null) {
          sAlluxioEtcdClient = new AlluxioEtcdClient(conf);
        }
      }
    }
    return sAlluxioEtcdClient;
  }

  @FunctionalInterface
  protected interface EtcdUtilCallable<V> {
    /**
     * The task where logics to communicate with etcd happens.
     *
     * @return Etcd gRPC call result
     * @throws Exception
     */
    V call() throws Exception;
  }

  private <V> V retryInternal(String description, RetryPolicy retryPolicy,
                              EtcdUtilCallable<V> etcdCallable) {
    Exception ex = null;
    // TODO(lucy) Currently retry on all sorts of exception and report the last exception,
    // As jetcd exception often hides underneath CompletableFuture.get(), find
    // more info later on distinguishing different possible exceptions.
    while (retryPolicy.attempt()) {
      try {
        return etcdCallable.call();
      } catch (Exception e) {
        // TODO(lucy) check in future if io.etcd.jetcd.common.exception.EtcdException
        // needs to be handled for detection of underlying jetcd client connection
        // getting into possible stale states or needs reconnection, for now, reuse single
        // jetcd client handle for entire lifecycle of AlluxioEtcdClient.
        LOG.warn("Failed to {} (attempt {}): {}", description, retryPolicy.getAttemptCount(),
            e.toString());
        ex = e;
      }
      LOG.debug("AlluxioEtcdClient call failed ({}): ", retryPolicy.getAttemptCount(), ex);
    }
    throw new UnavailableRuntimeException(
        String.format("Exhausted retry for (%s), retries:%s, last exception:",
            description, retryPolicy.getAttemptCount()),
        ex);
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

    /**
     * CTOR for Lease.
     * @param leaseId
     * @param ttlInSec
     */
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

  /**
   * Create a lease with timeout and ttl.
   * @param ttlInSec
   * @param timeout
   * @param timeUnit
   * @return Lease
   */
  public Lease createLease(long ttlInSec, long timeout, TimeUnit timeUnit) {
    return retryInternal(
        String.format("Creating Lease with ttl:%s", ttlInSec),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          CompletableFuture<LeaseGrantResponse> leaseGrantFut =
              getEtcdClient().getLeaseClient().grant(ttlInSec, timeout, timeUnit);
          LeaseGrantResponse resp = leaseGrantFut.get(timeout, timeUnit);
          long leaseId = resp.getID();
          Lease lease = new Lease(leaseId, ttlInSec);
          return lease;
        });
  }

  /**
   * Create lease with default ttl and timeout.
   * @return Lease
   */
  public Lease createLease() {
    return createLease(DEFAULT_LEASE_TTL_IN_SEC, DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
  }

  /**
   * Revoke given lease.
   * @param lease
   */
  public void revokeLease(Lease lease) {
    retryInternal(
        String.format("Revoking Lease:%s", lease.toString()),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          CompletableFuture<LeaseRevokeResponse> leaseRevokeFut =
              getEtcdClient().getLeaseClient().revoke(lease.mLeaseId);
          leaseRevokeFut.get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          return null;
        });
  }

  /**
   * Check with etcd if a lease is already expired.
   * @param lease
   * @return lease expired
   */
  public boolean isLeaseExpired(Lease lease) {
    return retryInternal(
        String.format("Checking IsLeaseExpired, lease:%s", lease.toString()),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          LeaseTimeToLiveResponse leaseResp = mClient.getLeaseClient()
              .timeToLive(lease.mLeaseId, LeaseOption.DEFAULT)
              .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          // if no such lease, lease resp will still be returned with a negative ttl
          return leaseResp.getTTl() <= 0;
        });
  }

  /**
   * Create a childPath with value to a parentPath.
   * e.g. create "lower_path" under path /upper_path/ to form a
   * kv pair of /upper_path/lower_path with a given value.
   * @param parentPath
   * @param childPath
   * @param value
   */
  public void addChildren(String parentPath, String childPath, byte[] value) {
    Preconditions.checkArgument(!StringUtil.isNullOrEmpty(parentPath));
    Preconditions.checkArgument(!StringUtil.isNullOrEmpty(childPath));
    String fullPath = PathUtils.concatPath(parentPath, childPath);
    Preconditions.checkArgument(!StringUtil.isNullOrEmpty(fullPath));
    retryInternal(
        String.format("Adding child for parentPath:%s, childPath:%s",
            parentPath, childPath),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, 0),
        () -> {
          return mClient.getKVClient().put(
                  ByteSequence.from(fullPath, StandardCharsets.UTF_8),
                  ByteSequence.from(value))
              .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        });
  }

  /**
   * Get list of children path kv pairs from a given parentPath
   * e.g. get [/upper/lower1 - val1, /upper/lower2 - val2]
   * under parent path /upper/
   * @param parentPath parentPath ends with /
   * @return list of children KeyValues
   */
  public List<KeyValue> getChildren(String parentPath) {
    Preconditions.checkArgument(!StringUtil.isNullOrEmpty(parentPath));
    return retryInternal(
        String.format("Getting children for path:%s", parentPath),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          GetResponse getResponse = mClient.getKVClient().get(
                  ByteSequence.from(parentPath, StandardCharsets.UTF_8),
                  GetOption.newBuilder().isPrefix(true).build())
              .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          return getResponse.getKvs();
        });
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
      LOG.warn("Watcher already there for path:{} for children.", parentPath);
      return;
    }
    WatchOption.Builder watchOptBuilder = WatchOption.newBuilder();
    switch (watchType) {
      /* e.g. Given the parentPath '/parent/',
      give query-like syntax equivalent to:
        select * with value < '/parent0' ('0' the char after '/' in ASCII)
      since everything prefixed with '/parent/' is strictly smaller than '/parent0'
      Example: with list of keys ['/parent-1', '/parent/k1','/parent/~']
      this query with keyRangeEnd = '/parent0' will result with ['/parent/k1', '/parent/~']
      since '/parent-1' is not prefixed with '/parent/'
      and '/parent/~' is the largest below '/parent0'
      */
      case CHILDREN:
        String keyRangeEnd = parentPath.substring(0, parentPath.length() - 1)
            + (char) (parentPath.charAt(parentPath.length() - 1) + 1);
        watchOptBuilder.isPrefix(true)
            .withRange(ByteSequence.from(keyRangeEnd, StandardCharsets.UTF_8));
        break;
      case SINGLE_PATH: // no need to add anything to watchoption, fall through.
      default:
        break;
    }

    Watch.Watcher watcher = retryInternal(
        String.format("Adding listener for path:%s, type:%s", parentPath, watchType),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, 0),
        () -> {
          Watch.Watcher newWatcher = mClient.getWatchClient().watch(
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
                      case UNRECOGNIZED: // Fall through
                      default:
                        LOG.info("Unrecognized event:{} on watch path of:{}",
                            event.getEventType(), parentPath);
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
          return newWatcher;
        });
    Watch.Watcher prevWatcher = mRegisteredWatchers.putIfAbsent(
        getRegisterWatcherKey(parentPath, watchType), watcher);
    // another same watcher already added in a race, close current one
    if (prevWatcher != null) {
      watcher.close();
    }
  }

  /**
   * Get the registered watch key in the map.
   * @param path
   * @param type
   * @return key for registered watcher
   */
  private static String getRegisterWatcherKey(String path, WatchType type) {
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
   * Get latest value attached to the path.
   * @param path
   * @return byte[] value
   */
  public byte[] getForPath(String path) {
    return retryInternal(
        String.format("Get for path:%s", path),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
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
        });
  }

  /**
   * Check existence of a single given path.
   * @param path
   * @return if the path exists or not
   */
  public boolean checkExistsForPath(String path) {
    return retryInternal(String.format("Check exists for path:%s", path),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          boolean exist = false;
          CompletableFuture<GetResponse> getResponse =
              getEtcdClient().getKVClient().get(
                  ByteSequence.from(path, StandardCharsets.UTF_8));
          List<KeyValue> kvs = getResponse.get(
              DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS).getKvs();
          exist = !kvs.isEmpty();
          return exist;
        });
  }

  /**
   * Create a path with given value in non-transactional way.
   * @param path
   * @param value
   */
  public void createForPath(String path, Optional<byte[]> value) {
    retryInternal(String.format("Create for path:%s, value bytes len:%s",
            path, (!value.isPresent() ? "null" : value.get().length)),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          mClient.getKVClient().put(
                  ByteSequence.from(path, StandardCharsets.UTF_8),
                  ByteSequence.from(value.get()))
              .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          return null;
        });
  }

  /**
   * Delete a path or recursively all paths with given path as prefix.
   * @param path
   * @param recursive
   */
  public void deleteForPath(String path, boolean recursive) {
    retryInternal(
        String.format("Delete for path:%s", path),
        new ExponentialBackoffRetry(RETRY_SLEEP_IN_MS, MAX_RETRY_SLEEP_IN_MS, RETRY_TIMES),
        () -> {
          mClient.getKVClient().delete(
                  ByteSequence.from(path, StandardCharsets.UTF_8),
                  DeleteOption.newBuilder().isPrefix(recursive).build())
              .get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          return null;
        });
  }

  /**
   * Remove listener on given path.
   * @param path
   * @param watchType
   */
  public void removeListenerInternal(String path, WatchType watchType) {
    Watch.Watcher watcher = mRegisteredWatchers.remove(getRegisterWatcherKey(path, watchType));
    if (watcher == null) {
      return;
    }
    watcher.close();
  }

  /**
   * Get the jetcd client instance.
   * @return jetcd client
   */
  public Client getEtcdClient() {
    return mClient;
  }
}
