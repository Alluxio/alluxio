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

import alluxio.Constants;
import alluxio.util.io.PathUtils;
import alluxio.worker.Worker;
import alluxio.worker.WorkerContext;
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
   * subdir of its {@link StorageDir}, the subdir is tmpFolder/sessionId % maxSubdirMax.
   * tmpFolder is {@link Constants#WORKER_DATA_TMP_FOLDER}.
   * maxSubdirMax is {@link Constants#WORKER_DATA_TMP_SUBDIR_MAX}.
   * The block file name is "sessionId-blockId". e.g. sessionId 2 creates a temp Block 100 in
   * {@link StorageDir} "/mnt/mem/0", this temp block has path:
   * <p>
   * /mnt/mem/0/.tmp_blocks/2/2-100
   *
   * @param dir the parent directory
   * @param sessionId the session id
   * @param blockId the block id
   * @return temp file path
   */
  public static String tempPath(StorageDir dir, long sessionId, long blockId) {
    final String tmpDir = WorkerContext.getConf().get(Constants.WORKER_DATA_TMP_FOLDER);
    final int subDirMax = WorkerContext.getConf().getInt(Constants.WORKER_DATA_TMP_SUBDIR_MAX);
    Preconditions.checkState(subDirMax > 0);

    return PathUtils.concatPath(dir.getDirPath(), tmpDir, sessionId % subDirMax,
        String.format("%x-%x", sessionId, blockId));
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
