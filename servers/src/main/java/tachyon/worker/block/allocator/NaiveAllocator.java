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

package tachyon.worker.block.allocator;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.meta.BlockMeta;

/**
 * Naive allocation strategy
 */
public class NaiveAllocator implements Allocator {
  private final BlockMetadataManager mMetadata;

  public NaiveAllocator(BlockMetadataManager metadata) {
    mMetadata = Preconditions.checkNotNull(metadata);
  }

  @Override
  public Optional<BlockMeta> allocateBlock(long userId, long blockId, long blockSize, int tierHint) {
    return mMetadata.addBlockMetaInTier(userId, blockId, blockSize, tierHint);
  }
}
