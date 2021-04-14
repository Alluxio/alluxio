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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the metadata of an uncommitted block in Alluxio managed storage.
 */
@NotThreadSafe
public final class DefaultTempBlockMeta implements TempBlockMeta {
  private final long mBlockId;
  private final StorageDir mDir;
  private final long mSessionId;
  private long mTempBlockSize;

  /**
   * All blocks are created as temp blocks before committed. They are stored in BlockStore under a
   * subdir of its {@link StorageDir}, the subdir is tmpFolder/sessionId % maxSubdirMax.
   * tmpFolder is a property of {@link PropertyKey#WORKER_DATA_TMP_FOLDER}.
   * maxSubdirMax is a property of {@link PropertyKey#WORKER_DATA_TMP_SUBDIR_MAX}.
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
    final String tmpDir = ServerConfiguration.get(PropertyKey.WORKER_DATA_TMP_FOLDER);
    final int subDirMax = ServerConfiguration.getInt(PropertyKey.WORKER_DATA_TMP_SUBDIR_MAX);

    return PathUtils.concatPath(dir.getDirPath(), tmpDir, sessionId % subDirMax,
        String.format("%x-%x", sessionId, blockId));
  }

  /**
   * Creates a new instance of {@link DefaultTempBlockMeta}.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param initialBlockSize initial size of this block in bytes
   * @param dir {@link StorageDir} of this temp block belonging to
   */
  public DefaultTempBlockMeta(long sessionId, long blockId, long initialBlockSize, StorageDir dir) {
    mBlockId = blockId;
    mDir = Preconditions.checkNotNull(dir, "dir");
    mSessionId = sessionId;
    mTempBlockSize = initialBlockSize;
  }

  @Override
  public long getBlockSize() {
    return mTempBlockSize;
  }

  @Override
  public String getPath() {
    return tempPath(mDir, mSessionId, mBlockId);
  }

  @Override
  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public BlockStoreLocation getBlockLocation() {
    StorageTier tier = mDir.getParentTier();
    return new BlockStoreLocation(tier.getTierAlias(), mDir.getDirIndex(), mDir.getDirMedium());
  }

  @Override
  public StorageDir getParentDir() {
    return mDir;
  }

  @Override
  public String getCommitPath() {
    return DefaultBlockMeta.commitPath(mDir, mBlockId);
  }

  @Override
  public long getSessionId() {
    return mSessionId;
  }

  @Override
  public void setBlockSize(long newSize) {
    mTempBlockSize = newSize;
  }
}
