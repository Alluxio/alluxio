package alluxio.master.file.meta;

import alluxio.master.file.meta.crosscluster.InvalidationSyncCache;
import alluxio.master.file.meta.options.MountInfo;
import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps mount Ids to SyncPathCache.
 */
public class SyncCacheMap {
  private final UfsSyncPathCache mBaseCache = new UfsSyncPathCache();
  private final ConcurrentHashMap<Long, SyncPathCache> mCacheMap = new ConcurrentHashMap<>();

  /**
   * @param mountId the mount Id
   * @return the cache associated with the id
   */
  public SyncPathCache getCacheByMountId(long mountId) {
    SyncPathCache cache = mCacheMap.get(mountId);
    if (cache != null) {
      return cache;
    }
    return mBaseCache;
  }

  /**
   * Called when a new mount is added.
   * @param info the mount info
   */
  public void addMount(MountInfo info) {
    if (info.getOptions().getCrossCluster()) {
      mCacheMap.compute(info.getMountId(), (key, cache) -> {
        Preconditions.checkState(cache == null,
            "tried to mount with existing cache");
        return new InvalidationSyncCache();
      });
    }
  }

  /**
   * Called when removing an existing mount.
   * @param info the mount info
   */
  public void removeMount(MountInfo info) {
    if (info.getOptions().getCrossCluster()) {
      mCacheMap.compute(info.getMountId(), (key, cache) -> {
        Preconditions.checkState(cache != null,
            "tried to remove non-existing mount cache");
        return null;
      });
    }
  }

  /**
   * Called when updating an existing mount.
   * @param oldInfo the old mount info
   * @param newInfo the new mount info
   */
  public void updateMount(MountInfo oldInfo, MountInfo newInfo) {
    if (oldInfo.getOptions().getCrossCluster()) {
      removeMount(oldInfo);
    }
    addMount(newInfo);
  }
}
