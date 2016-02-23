/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.StorageDirView;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is used to evict old blocks in certain StorageDir by LRU. The main difference
 * between PartialLRU and LRU is that LRU choose old blocks among several
 * {@link alluxio.worker.block.meta.StorageDir}s until one StorageDir satisfies the request space,
 * but PartialLRU select one StorageDir with maximum free space first and evict old blocks in the
 * selected StorageDir by LRU
 */
@NotThreadSafe
public class PartialLRUEvictor extends LRUEvictor {

  /**
   * Creates a new instance of {@link PartialLRUEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public PartialLRUEvictor(BlockMetadataManagerView view, Allocator allocator) {
    super(view, allocator);
  }

  @Override
  protected BlockStoreLocation updateBlockStoreLocation(long bytesToBeAvailable,
      BlockStoreLocation location) {
    StorageDirView candidateDirView =
        EvictorUtils.getDirWithMaxFreeSpace(bytesToBeAvailable, location, mManagerView);
    if (candidateDirView != null) {
      return new BlockStoreLocation(location.tierAlias(), candidateDirView.getDirViewIndex());
    } else {
      return location;
    }
  }
}
