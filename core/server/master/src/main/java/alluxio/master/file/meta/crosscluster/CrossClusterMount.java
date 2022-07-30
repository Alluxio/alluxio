package alluxio.master.file.meta.crosscluster;

import alluxio.AlluxioURI;
import alluxio.conf.path.TrieNode;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.MountList;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.UfsInfo;
import alluxio.master.file.meta.options.MountInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.Streams;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tracks the mounts of other clusters, subscribing to them on changes.
 */
public class CrossClusterMount {

  private final TrieNode<Set<MountSync>> mExternalMounts = new TrieNode<>();
  private final Map<String, Set<MountSync>> mExternalMountsMap = new HashMap<>();
  private final HashSet<MountSync> mLocalMounts = new HashSet<>();
  private final Consumer<StreamObserver<PathInvalidation>> mOnStreamCreation;
  private final Consumer<StreamObserver<PathInvalidation>> mOnStreamCancellation;

  private final Map<MountSync, StreamObserver<PathInvalidation>> mActiveSubscriptions
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
                           Consumer<StreamObserver<PathInvalidation>> onStreamCreation,
                           Consumer<StreamObserver<PathInvalidation>> onStreamCancellation) {
    mSyncCache = syncCache;
    mLocalClusterId = localClusterId;
    mOnStreamCreation = onStreamCreation;
    mOnStreamCancellation = onStreamCancellation;
  }

  /**
   * @return the currently active streams
   */
  @VisibleForTesting
  public Set<MountSync> getActiveSubscriptions() {
    return new HashSet<>(mActiveSubscriptions.keySet());
  }

  @VisibleForTesting
  static class InvalidationStream implements StreamObserver<PathInvalidation> {

    private static final Logger LOG = LoggerFactory.getLogger(InvalidationStream.class);

    InvalidationSyncCache mInvalidationCache;
    MountSync mMountSync;
    CrossClusterMount mCrossClusterMount;

    InvalidationStream(MountSync mount, InvalidationSyncCache invalidationCache,
                       CrossClusterMount crossClusterMount) {
      mInvalidationCache = invalidationCache;
      mMountSync = mount;
      mCrossClusterMount = crossClusterMount;
    }

    /**
     * @return the mount info for the stream
     */
    @VisibleForTesting
    public MountSync getMountSync() {
      return mMountSync;
    }

    @Override
    public void onNext(PathInvalidation invalidation) {
      try {
        mInvalidationCache.notifyInvalidation(new AlluxioURI(invalidation.getUfsPath()));
      } catch (InvalidPathException e) {
        LOG.warn("Received invalid invalidation path", e);
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.warn("Error in path invalidation stream", t);
      mCrossClusterMount.removeStream(mMountSync, this);
    }

    @Override
    public void onCompleted() {
      mCrossClusterMount.removeStream(mMountSync, this);
    }
  }

  /**
   * Basic mount information.
   */
  public static class MountSync {
    final String mClusterId;
    final String mUfsPath;

    static MountSync fromMountInfo(String clusterId, MountInfo info) {
      return new MountSync(clusterId,
          info.getUfsUri().toString());
    }

    static MountSync fromUfsInfo(String clusterId, UfsInfo info) {
      return new MountSync(clusterId, info.getUri());
    }

    /**
     * Create a new mount sync object.
     * @param clusterId the cluster id
     * @param ufsPath the ufs path
     */
    @VisibleForTesting
    public MountSync(String clusterId, String ufsPath) {
      mClusterId = clusterId;
      mUfsPath = ufsPath;
    }

    /**
     * @return the cluster id
     */
    public String getClusterId() {
      return mClusterId;
    }

    /**
     * @return the ufs path
     */
    public String getUfsPath() {
      return mUfsPath;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof MountSync) {
        MountSync other =  (MountSync) o;
        return mClusterId.equals(other.mClusterId) && mUfsPath.equals(other.mUfsPath);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mClusterId, mUfsPath);
    }
  }

  private void checkMounts() {
    // first compute the set of intersecting mounts
    Stream<MountSync> mountStream = Stream.empty();
    for (MountSync mount : mLocalMounts) {
      mountStream = Streams.concat(mountStream, mExternalMounts.search(mount.mUfsPath).stream()
              .flatMap((node) ->
                  node.getValue().stream().map(mountSync ->
                      new MountSync(mountSync.mClusterId, mount.mUfsPath))),
              mExternalMounts.getChildren(mount.mUfsPath)
                  .flatMap((node) -> node.getValue().stream()));
    }
    Set<MountSync> mounts = mountStream.collect(Collectors.toSet());

    // remove any mounts that no longer exist
    for (Map.Entry<MountSync, StreamObserver<PathInvalidation>> entry
        : mActiveSubscriptions.entrySet()) {
      if (!mounts.contains(entry.getKey())) {
        mOnStreamCancellation.accept(
            Verify.verifyNotNull(mActiveSubscriptions.remove(entry.getKey())));
      }
    }

    // add any new mounts
    for (MountSync mount : mounts) {
      if (!mActiveSubscriptions.containsKey(mount)) {
        InvalidationStream stream = new InvalidationStream(mount, mSyncCache, this);
        mOnStreamCreation.accept(stream);
        mActiveSubscriptions.put(mount, stream);
      }
    }
  }

  private synchronized void removeStream(
      MountSync mount, StreamObserver<PathInvalidation> invalidationStream) {
    StreamObserver<PathInvalidation> otherStream = mActiveSubscriptions.get(mount);
    if (otherStream == invalidationStream) {
      mActiveSubscriptions.remove(mount);
      checkMounts();
    }
  }

  /**
   * Add a local mount.
   * @param mount the mount
   */
  public synchronized void addLocalMount(MountInfo mount) {
    mLocalMounts.add(MountSync.fromMountInfo(mLocalClusterId, mount));
    checkMounts();
  }

  /**
   * Remove a local mount.
   * @param mount the mount
   */
  public synchronized void removeLocalMount(MountInfo mount) {
    Verify.verify(mLocalMounts.remove(MountSync.fromMountInfo(mLocalClusterId, mount)),
        "tried to remove a non existing local mount");
    checkMounts();
  }

  /**
   * Set the list of mounts for an external cluster.
   * @param list the list of mounts
   */
  public synchronized void setExternalMountList(MountList list) {
    if (list.getClusterId().equals(mLocalClusterId)) {
      throw new IllegalStateException(
          "External mount has same cluster id as local" + mLocalClusterId);
    }

    // first delete any existing mounts for the cluster id
    Set<MountSync> mounts = mExternalMountsMap.remove(list.getClusterId());
    if (mounts != null) {
      for (MountSync mount : mounts) {
        mExternalMounts.deleteIf(mount.mUfsPath, (node) -> {
          Verify.verify(node.getValue().remove(mount),
              "tried to remove a non existing remote mount");
          return node.getValue().isEmpty();
        });
      }
    }
    // now add any new mounts for the cluster id (but filter read only mounts)
    Set<MountSync> newMounts = list.getMountsList().stream()
        .filter((mount) -> !mount.getProperties().getReadOnly()).map((mount) ->
        MountSync.fromUfsInfo(list.getClusterId(), mount)).collect(Collectors.toSet());
    mExternalMountsMap.put(list.getClusterId(), newMounts);
    for (MountSync mount : newMounts) {
      TrieNode<Set<MountSync>> node = mExternalMounts.insert(mount.mUfsPath);
      Set<MountSync> mountSet = node.getValue();
      if (mountSet == null) {
        mountSet = new HashSet<>();
        node.setValue(mountSet);
      }
      mountSet.add(mount);
    }
    // now compute any subscription changes
    checkMounts();
  }
}
