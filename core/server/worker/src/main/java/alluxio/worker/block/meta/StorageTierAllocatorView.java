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

package alluxio.worker.block.meta;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a wrapper of {@link StorageTier} to provide more limited access for allocators.
 */
@ThreadSafe
public class StorageTierAllocatorView extends StorageTierView {

  /**
   * Creates a {@link StorageTierView} using the actual {@link StorageTier}.
   *
   * @param tier which the tierView is constructed from
   * @param useReservedSpace whether to include reserved space in dir's available bytes
   */
  public StorageTierAllocatorView(StorageTier tier, boolean useReservedSpace) {
    super(tier, useReservedSpace);
    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirAllocatorView dirView = new StorageDirAllocatorView(dir, this);
      mDirViews.put(dirView.getDirViewIndex(), dirView);
    }
  }
}
