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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the metadata of an uncommitted block in Alluxio managed storage.
 */
@NotThreadSafe
public final class TempBlockMeta extends AbstractBlockMeta {
  private final long mSessionId;
  private long mTempBlockSize;

  /**
   * Creates a new instance of {@link TempBlockMeta}.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param initialBlockSize initial size of this block in bytes
   * @param dir {@link StorageDir} of this temp block belonging to
   */
  public TempBlockMeta(long sessionId, long blockId, long initialBlockSize, StorageDir dir) {
    super(blockId, dir);
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

  /**
   * @return the commit path
   */
  public String getCommitPath() {
    return commitPath(mDir, mBlockId);
  }

  /**
   * @return the session id
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @param newSize block size to use
   */
  public void setBlockSize(long newSize) {
    mTempBlockSize = newSize;
  }
}
