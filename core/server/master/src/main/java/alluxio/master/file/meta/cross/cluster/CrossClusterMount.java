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

package alluxio.master.file.meta.cross.cluster;

import alluxio.AlluxioURI;
import alluxio.conf.path.TrieNode;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.MountList;
import alluxio.grpc.NetAddress;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.RemovedMount;
import alluxio.master.file.meta.options.MountInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.Streams;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tracks the mounts of other clusters, subscribing to them on changes.
 */
public class CrossClusterMount implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMount.class);

  private final TrieNode<Set<MountSyncAddress>> mExternalMounts = new TrieNode<>();
  private final TrieNode<Map<String, RemovedMount>> mRemovedMounts = new TrieNode<>();
  private final Map<String, Set<MountSyncAddress>> mExternalMountsMap = new HashMap<>();
  private final Map<String, Set<InetSocketAddress>> mExternalClusterAddresses = new HashMap<>();
  private final HashSet<MountSync> mLocalMounts = new HashSet<>();
  private final Consumer<InvalidationStream> mOnStreamCreation;
  private final Consumer<InvalidationStream> mOnStreamCancellation;
  private final CrossClusterConnections mConnections = new CrossClusterConnections();

  private final Map<MountSyncAddress, InvalidationStream> mActiveSubscriptions
      = new HashMap<>();
  private final InvalidationSyncCache mSyncCache;
  private final String mLocalClusterId;

  /**
   * Create a new cross cluster mount.
   * @param localClusterId the local cluster id
   * @param syncCache the sync cache
   * @param onStreamCreation called when a new stream is created
   * @param onStreamCancellation called when a stream is cancelled
   */
  public CrossClusterMount(String localClusterId, InvalidationSyncCache syncCache,
                           Consumer<InvalidationStream> onStreamCreation,
                           Consumer<InvalidationStream> onStreamCancellation) {
    mSyncCache = syncCache;
    mLocalClusterId = localClusterId;
    mOnStreamCreation = onStreamCreation;
    mOnStreamCancellation = onStreamCancellation;
  }

  /**
   * @return the currently active streams
   */
  @VisibleForTesting
  public Set<MountSyncAddress> getActiveSubscriptions() {
    return new HashSet<>(mActiveSubscriptions.keySet());
  }

  @Override
  public synchronized void close() throws IOException {
    LOG.info("Closing cross cluster subscriptions");
    mActiveSubscriptions.entrySet().removeIf((entry) -> {
      entry.getValue().cancel();
      mOnStreamCancellation.accept(entry.getValue());
      return true;
    });
  }

  /**
   * This will check if any of the active subscriptions differ from those needed by the current
   * state of external and local mounts, creating or removing subscriptions as necessary.
   */
  private void checkActiveSubscriptions() {
    // first compute the set of intersecting mounts
    Stream<MountSyncAddress> mountStream = Stream.empty();
    for (MountSync mount : mLocalMounts) {
      // The external mounts that are prefixes of the local mount (i.e. those returned by
      // mExternalMounts.search), will be subscribed to using the path of the local mount.
      // The external mounts of which the local mount is a prefix (i.e. those returned by
      // mExternalMounts.getChildren) will be subscribed to using the path of the
      // external mount.
      mountStream = Streams.concat(mountStream, mExternalMounts.search(mount.getUfsPath()).stream()
              .flatMap((node) ->
                  node.getValue().stream().map(mountSync ->
                      new MountSyncAddress(
                          new MountSync(mountSync.getMountSync().getClusterId(),
                              mount.getUfsPath()), mountSync.getAddresses()))),
              mExternalMounts.getChildren(mount.getUfsPath())
                  .flatMap((node) -> node.getValue().stream()));
    }
    Set<MountSyncAddress> mounts = mountStream.collect(Collectors.toSet());

    Set<Set<InetSocketAddress>> mountAddresses = mounts.stream().map(
        mount -> new HashSet<>(Arrays.asList(mount.getAddresses()))).collect(Collectors.toSet());
    Set<Set<InetSocketAddress>> removedMountAddresses = new HashSet<>();
    // remove any mount subscriptions that no longer exist
    mActiveSubscriptions.entrySet().removeIf((entry) -> {
      if (!mounts.contains(entry.getKey())) {
        LOG.info("Ending cross cluster subscription to {}", entry.getKey());
        entry.getValue().cancel();
        mOnStreamCancellation.accept(entry.getValue());
        removedMountAddresses.add(new HashSet<>(Arrays.asList(entry.getKey().getAddresses())));
        return true;
      }
      return false;
    });

    // add any new mount subscriptions
    for (MountSyncAddress mount : mounts) {
      if (!mActiveSubscriptions.containsKey(mount)) {
        LOG.info("Creating cross cluster subscription to {}", mount);
        InvalidationStream stream = new InvalidationStream(mount, mSyncCache, this);
        mConnections.addStream(stream);
        mOnStreamCreation.accept(stream);
        mActiveSubscriptions.put(mount, stream);
      }
    }
    // close the connection of any address sets that are no longer used
    for (Set<InetSocketAddress> addressSet : removedMountAddresses) {
      if (!mountAddresses.contains(addressSet)) {
        mConnections.removeClient(addressSet);
      }
    }
  }

  synchronized void removeStream(
      MountSyncAddress mount, StreamObserver<PathInvalidation> invalidationStream) {
    StreamObserver<PathInvalidation> otherStream = mActiveSubscriptions.get(mount);
    if (otherStream == invalidationStream) {
      mActiveSubscriptions.remove(mount);
      checkActiveSubscriptions();
    }
  }

  /**
   * Add a local mount.
   * @param mount the mount
   */
  public synchronized void addLocalMount(MountInfo mount) {
    LOG.info("Adding local mount {} for cross cluster subscriptions", mount);
    mLocalMounts.add(MountSync.fromMountInfo(mLocalClusterId, mount));
    checkActiveSubscriptions();
  }

  /**
   * Remove a local mount.
   * @param mount the mount
   */
  public synchronized void removeLocalMount(MountInfo mount) {
    LOG.info("Removing local mount {} for cross cluster subscriptions", mount);
    Verify.verify(mLocalMounts.remove(MountSync.fromMountInfo(mLocalClusterId, mount)),
        "tried to remove a non existing local mount");
    checkActiveSubscriptions();
  }

  /**
   * Set the list of mounts for an external cluster.
   * @param list the list of mounts
   */
  public synchronized void setExternalMountList(MountList list) {
    LOG.info("Setting external mount list {} for cross cluster subscriptions", list);
    if (list.getClusterId().equals(mLocalClusterId)) {
      throw new IllegalStateException(
          "External mount has same cluster id as local" + mLocalClusterId);
    }

    // Check if the address list for the cluster id has changed,
    // so we can remove the old connection
    Set<InetSocketAddress> newAddresses = toAddressStream(list.getAddressesList())
        .collect(Collectors.toSet());
    Set<InetSocketAddress> oldAddresses = mExternalClusterAddresses.get(list.getClusterId());
    if (oldAddresses != null) {
      if (!oldAddresses.equals(newAddresses)) {
        mConnections.removeClient(oldAddresses);
      }
    }
    mExternalClusterAddresses.put(list.getClusterId(), newAddresses);

    // check for any newly removed external mounts, and update the invalidation cache for these
    // as we might need to sync paths that were previously mounted then removed externally
    // (we only update the invalidation cache here, and not when a local mount changes, because
    // a local mount change will force a local sync when its subscription starts)
    for (RemovedMount mount : list.getRemovedMountsList()) {
      TrieNode<Map<String, RemovedMount>> node = mRemovedMounts.insert(mount.getUfsPath());
      Map<String, RemovedMount> removed = node.getValue();
      if (removed == null) {
        removed = new HashMap<>();
        node.setValue(removed);
      }
      removed.compute(list.getClusterId(), (key, prevRemoved) -> {
        if (prevRemoved == null || prevRemoved.getTime() < mount.getTime()) {
          for (MountSync local : mLocalMounts) {
            try {
              if (local.getUfsPath().startsWith(mount.getUfsPath())) {
                mSyncCache.notifyUfsInvalidation(new AlluxioURI(local.getUfsPath()));
              } else if (mount.getUfsPath().startsWith(local.getUfsPath())) {
                mSyncCache.notifyUfsInvalidation(new AlluxioURI(mount.getUfsPath()));
              }
            } catch (InvalidPathException e) {
              LOG.warn("Received an invalid removed mount {}", mount, e);
              return null;
            }
          }
          return mount;
        }
        return prevRemoved;
      });
    }

    // here we remove all mounts for cluster id, then add them all back, but using the new
    // information, following that we compute any changes with the list of active
    // subscriptions to the external cluster
    // first delete any existing mounts for the cluster id
    Set<MountSyncAddress> mounts = mExternalMountsMap.remove(list.getClusterId());
    if (mounts != null) {
      for (MountSyncAddress mount : mounts) {
        mExternalMounts.deleteIf(mount.getMountSync().getUfsPath(), (node) -> {
          Verify.verify(node.getValue().remove(mount),
              "tried to remove a non existing remote mount");
          return node.getValue().isEmpty();
        });
      }
    }
    // now add any new mounts for the cluster id (but filter read only mounts)
    Set<MountSyncAddress> newMounts = list.getMountsList().stream()
        .filter((mount) -> !mount.getProperties().getReadOnly()).map((mount) ->
        new MountSyncAddress(MountSync.fromUfsInfo(list.getClusterId(), mount),
            toAddressStream(list.getAddressesList()).toArray(InetSocketAddress[]::new)))
        .collect(Collectors.toSet());
    mExternalMountsMap.put(list.getClusterId(), newMounts);
    for (MountSyncAddress mount : newMounts) {
      TrieNode<Set<MountSyncAddress>> node =
          mExternalMounts.insert(mount.getMountSync().getUfsPath());
      Set<MountSyncAddress> mountSet = node.getValue();
      if (mountSet == null) {
        mountSet = new HashSet<>();
        node.setValue(mountSet);
      }
      mountSet.add(mount);
    }
    // now compute any subscription changes
    checkActiveSubscriptions();
    // if there are no mounts for the address, then we can remove the connection
    if (newMounts.isEmpty()) {
      mConnections.removeClient(newAddresses);
    }
  }

  private static Stream<InetSocketAddress> toAddressStream(List<NetAddress> addressList) {
    return addressList.stream().map((address) ->
            new InetSocketAddress(address.getHost(), address.getRpcPort()));
  }

  /**
   * @return the object containing the connections to the other clusters
   */
  @VisibleForTesting
  public CrossClusterConnections getConnections() {
    return mConnections;
  }
}
