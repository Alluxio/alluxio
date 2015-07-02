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

import java.io.File;

/**
 * Represents the metadata of a block in Tachyon managed storage.
 */
public class BlockMeta extends BlockMetaBase {
  private final long mBlockSize;
  private long mLastAccessTimeMs;

  public BlockMeta(long blockId, long blockSize, StorageDir dir) {
    super(blockId, dir);
    mBlockSize = blockSize;
    mLastAccessTimeMs = System.currentTimeMillis();
  }

  public BlockMeta(TempBlockMeta tempBlock) {
    super(tempBlock.getBlockId(), tempBlock.getParentDir());
    mBlockSize = new File(tempBlock.getCommitPath()).length();
    mLastAccessTimeMs = System.currentTimeMillis();
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  @Override
  public String getPath() {
    return commitPath();
  }

  public void setLastAccessTimeMs(long lastAccessTimeMs) {
    mLastAccessTimeMs = lastAccessTimeMs;
  }
}
