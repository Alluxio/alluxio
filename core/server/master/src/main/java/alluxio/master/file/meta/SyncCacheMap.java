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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.master.file.meta.cross.cluster.InvalidationSyncCache;
import alluxio.master.file.meta.options.MountInfo;

import com.google.common.base.Verify;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Maps mount Ids to SyncPathCache.
 */
public class SyncCacheMap {
  private final UfsSyncPathCache mBaseCache = new UfsSyncPathCache();
  private final Set<Long> mCacheSet = new ConcurrentHashSet<>();
  private final InvalidationSyncCache mInvalidationCache;

  /**
   * @param reverseResolution function from ufs path to alluxio path
   */
  public SyncCacheMap(Function<AlluxioURI, Optional<AlluxioURI>> reverseResolution) {
    mInvalidationCache = new InvalidationSyncCache(reverseResolution);
  }

  /**
   * @param mountId the mount id
   * @return the cache associated with the id
   */
  public SyncPathCache getCacheByMountId(long mountId) {
    if (mCacheSet.contains(mountId)) {
      return mInvalidationCache;
    }
    return mBaseCache;
  }

  InvalidationSyncCache getInvalidationCache() {
    return mInvalidationCache;
  }

  /**
   * Called when a new mount is added.
   * @param info the mount info
   */
  public void addMount(MountInfo info) {
    if (info.getOptions().getCrossCluster()) {
      mCacheSet.add(info.getMountId());
    }
  }

  /**
   * Called when removing an existing mount.
   * @param info the mount info
   */
  public void removeMount(MountInfo info) {
    if (info.getOptions().getCrossCluster()) {
      Verify.verify(mCacheSet.remove(info.getMountId()),
          "tried to remove non-existing mount cache");
    }
  }
}
