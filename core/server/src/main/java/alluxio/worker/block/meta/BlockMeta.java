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

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the metadata of a block in Alluxio managed storage.
 */
@ThreadSafe
public final class BlockMeta extends BlockMetaBase {
  private final long mBlockSize;

  /**
   * Creates a new instance of {@link BlockMeta}.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param dir the parent directory
   */
  public BlockMeta(long blockId, long blockSize, StorageDir dir) {
    super(blockId, dir);
    mBlockSize = blockSize;
  }

  /**
   * Creates a new instance of {@link BlockMeta} from {@link TempBlockMeta}.
   *
   * @param tempBlock uncommitted block metadata
   */
  public BlockMeta(TempBlockMeta tempBlock) {
    super(tempBlock.getBlockId(), tempBlock.getParentDir());
    // NOTE: TempBlockMeta must be committed after the actual data block file is moved.
    mBlockSize = new File(tempBlock.getCommitPath()).length();
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  @Override
  public String getPath() {
    return commitPath(mDir, mBlockId);
  }
}
