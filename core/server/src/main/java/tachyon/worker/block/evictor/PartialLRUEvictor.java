/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.evictor;

import javax.annotation.concurrent.NotThreadSafe;

import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.meta.StorageDirView;

/**
 * This class is used to evict old blocks in certain StorageDir by LRU. The main difference
 * between PartialLRU and LRU is that LRU choose old blocks among several
 * {@link tachyon.worker.block.meta.StorageDir}s until one StorageDir satisfies the request space,
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
