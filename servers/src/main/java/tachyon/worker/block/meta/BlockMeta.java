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

import tachyon.util.CommonUtils;

/**
 * Represents the metadata of a block in Tachyon managed storage.
 */
public class BlockMeta {
  private final long mBlockId;
  private long mBlockSize;
  private StorageDir mDir;
  private boolean mCommitted = false;

  public BlockMeta(long blockId, long blockSize, StorageDir dir) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mDir = Preconditions.checkNotNull(dir);
  }

  public long getBlockId() {
    return mBlockId;
  }

  public long getBlockSize() {
    return mBlockSize;
  }

  public String getPath() {
    return CommonUtils.concatPath(mDir.getDirPath(), mBlockId);
  }

  public String getTmpPath(long userId) {
    return CommonUtils.concatPath(mDir.getDirPath(), userId, mBlockId);
  }

  public StorageDir getParentDir() {
    return mDir;
  }

  public boolean commit() {
    if (mCommitted) {
      return false;
    }
    mCommitted = true;
  }

  public boolean isCommitted() {
    return mCommitted;
  }

}
