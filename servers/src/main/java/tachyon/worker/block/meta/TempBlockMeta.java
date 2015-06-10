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

import tachyon.util.CommonUtils;

/**
 * Represents the metadata of an uncommited block in Tachyon managed storage.
 */
public class TempBlockMeta extends BlockMetaBase {
  private final long mUserId;

  public TempBlockMeta(long userId, long blockId, long blockSize, StorageDir dir) {
    super(blockId, blockSize, dir);
    mUserId = userId;
  }

  @Override
  public String getPath() {
    return CommonUtils.concatPath(mDir.getDirPath(), mUserId, mBlockId);
  }

  public String getCommitPath() {
    return CommonUtils.concatPath(mDir.getDirPath(), mBlockId);
  }

  public void setBlockSize(long newSize) {
    mBlockSize = newSize;
  }
}
