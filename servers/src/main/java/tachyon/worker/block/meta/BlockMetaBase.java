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

package tachyon.worker.block.meta;

import com.google.common.base.Preconditions;

import tachyon.util.io.PathUtils;
import tachyon.worker.block.BlockStoreLocation;

/**
 * A base class of the metadata of blocks in Tachyon managed storage.
 */
public abstract class BlockMetaBase {
  /**
   * All blocks are created as temp blocks before committed. They are stored in BlockStore under a
   * subdir of its StorageDir, the subdir is the same as the creator's sessionId, and the block file
   * is the same as its blockId. e.g. sessionId 2 creates a temp Block 100 in StorageDir
   * "/mnt/mem/0", this temp block has path:
   * <p>
   * /mnt/mem/0/2/100
   *
   * @return temp file path
   */
  public static String tempPath(StorageDir dir, long sessionId, long blockId) {
    return PathUtils.concatPath(dir.getDirPath(), sessionId, blockId);
  }

  /**
   * Committed block is stored in BlockStore under its StorageDir as a block file named after its
   * blockId. e.g. Block 100 of StorageDir "/mnt/mem/0" has path:
   * <p>
   * /mnt/mem/0/100
   *
   * @return committed file path
   */
  public static String commitPath(StorageDir dir, long blockId) {
    return PathUtils.concatPath(dir.getDirPath(), blockId);
  }

  protected final long mBlockId;
  protected final StorageDir mDir;

  public BlockMetaBase(long blockId, StorageDir dir) {
    mBlockId = blockId;
    mDir = Preconditions.checkNotNull(dir);
  }

  public long getBlockId() {
    return mBlockId;
  }

  /**
   * Get the location of a specific block
   */
  public BlockStoreLocation getBlockLocation() {
    StorageTier tier = mDir.getParentTier();
    return new BlockStoreLocation(tier.getTierAlias(), tier.getTierLevel(), mDir.getDirIndex());
  }

  public StorageDir getParentDir() {
    return mDir;
  }

  public abstract String getPath();

  public abstract long getBlockSize();
}
