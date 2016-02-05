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

package alluxio.worker.block.meta;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the metadata of an uncommitted block in Alluxio managed storage.
 */
@NotThreadSafe
public class TempBlockMeta extends BlockMetaBase {
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
