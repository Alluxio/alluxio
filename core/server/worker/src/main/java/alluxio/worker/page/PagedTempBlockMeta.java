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

package alluxio.worker.page;

import com.google.common.base.Preconditions;

/**
 * Temp block meta for a paged block.
 */
public class PagedTempBlockMeta extends PagedBlockMeta {
  private long mBlockSize;

  /**
   * @param blockId
   * @param dir
   */
  public PagedTempBlockMeta(long blockId, PagedBlockStoreDir dir) {
    super(blockId, 0, dir);
    mBlockSize = 0;
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @param newSize
   */
  public void setBlockSize(long newSize) {
    //Preconditions.checkState(mBlockSize <= newSize, "Shrinking block, not supported!");
    mBlockSize = newSize;
  }
}
