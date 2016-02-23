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

package alluxio.worker.block.meta;

import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A base class of the metadata of blocks in Alluxio managed storage.
 */
@ThreadSafe
public abstract class AbstractBlockMeta {
  /**
   * All blocks are created as temp blocks before committed. They are stored in BlockStore under a
   * subdir of its {@link StorageDir}, the subdir is the same as the creator's sessionId, and the
   * block file is the same as its blockId. e.g. sessionId 2 creates a temp Block 100 in
   * {@link StorageDir} "/mnt/mem/0", this temp block has path:
   * <p>
   * /mnt/mem/0/2/100
   *
   * @param dir the parent directory
   * @param sessionId the session id
   * @param blockId the block id
   * @return temp file path
   */
  public static String tempPath(StorageDir dir, long sessionId, long blockId) {
    return PathUtils.concatPath(dir.getDirPath(), sessionId, blockId);
  }

  /**
   * Committed block is stored in BlockStore under its {@link StorageDir} as a block file named
   * after its blockId. e.g. Block 100 of StorageDir "/mnt/mem/0" has path:
   * <p>
   * /mnt/mem/0/100
   *
   * @param blockId the block id
   * @param dir the parent directory
   * @return committed file path
   */
  public static String commitPath(StorageDir dir, long blockId) {
    return PathUtils.concatPath(dir.getDirPath(), blockId);
  }

  protected final long mBlockId;
  protected final StorageDir mDir;

  /**
   * Creates a new instance of {@link AbstractBlockMeta}.
   *
   * @param blockId the block id
   * @param dir the parent directory
   */
  public AbstractBlockMeta(long blockId, StorageDir dir) {
    mBlockId = blockId;
    mDir = Preconditions.checkNotNull(dir);
  }

  /**
   * @return the block id
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return location of the block
   */
  public BlockStoreLocation getBlockLocation() {
    StorageTier tier = mDir.getParentTier();
    return new BlockStoreLocation(tier.getTierAlias(), mDir.getDirIndex());
  }

  /**
   * @return the parent directory
   */
  public StorageDir getParentDir() {
    return mDir;
  }

  /**
   * @return the block path
   */
  public abstract String getPath();

  /**
   * @return the block size
   */
  public abstract long getBlockSize();
}
