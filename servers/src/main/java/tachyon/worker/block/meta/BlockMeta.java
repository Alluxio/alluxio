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

import java.util.UUID;

import tachyon.util.CommonUtils;

/**
 * Represents the metadata of a block in Tachyon managed storage.
 */
public class BlockMeta {
  private final long mBlockId;
  private final long mBlockSize;
  private final String mPath;
  private final String mTmpPath;

  public BlockMeta(long blockId, long blockSize, String localPath) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mPath = CommonUtils.concatPath(localPath, blockId);
    mTmpPath = CommonUtils.concatPath(localPath, UUID.randomUUID());
  }

  public long getBlockId() {
    return mBlockId;
  }

  public long getBlockSize() {
    return mBlockSize;
  }

  public String getPath() {
    return mPath;
  }

  public String getTmpPath() {
    return mTmpPath;
  }
}
