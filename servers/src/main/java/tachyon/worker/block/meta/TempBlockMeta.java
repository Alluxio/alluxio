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

/**
 * Represents the metadata of an uncommitted block in Tachyon managed storage.
 */
public class TempBlockMeta extends BlockMetaBase {
  private final long mUserId;
  private long mTempBlockSize;

  /**
   * Constructs the metadata of a temp block.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param initialBlockSize initial size of this block in bytes
   * @param dir StorageDir of this temp block belonging to
   */
  public TempBlockMeta(long userId, long blockId, long initialBlockSize, StorageDir dir) {
    super(blockId, dir);
    mUserId = userId;
    mTempBlockSize = initialBlockSize;
  }

  @Override
  public long getBlockSize() {
    return mTempBlockSize;
  }

  @Override
  public String getPath() {
    return tempPath(mDir, mUserId, mBlockId);
  }

  /**
   * @return the commit path
   */
  public String getCommitPath() {
    return commitPath(mDir, mBlockId);
  }

  /**
   * @return the user id
   */
  public long getUserId() {
    return mUserId;
  }

  /**
   * @param newSize block size to use
   */
  public void setBlockSize(long newSize) {
    mTempBlockSize = newSize;
  }
}
