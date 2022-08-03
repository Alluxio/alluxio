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

import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;

public class PagedBlockMeta implements BlockMeta {
  private final long mBlockId;
  private final long mBlockSize;
  private final PagedBlockStoreDir mParentDir;
  private final BlockStoreLocation mLocation;

  public PagedBlockMeta(long blockId, PagedBlockStoreDir dir, long blockSize) {
    mBlockId = blockId;
    mParentDir = dir;
    mBlockSize = blockSize;
    mLocation = new BlockStoreLocation(PagedBlockStoreTier.DEFAULT_TIER, dir.getDirIndex(),
        PagedBlockStoreTier.DEFAULT_MEDIUM);
  }

  @Override
  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public BlockStoreLocation getBlockLocation() {
    return mLocation;
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  @Override
  public StorageDir getParentDir() {
    return mParentDir;
  }

  @Override
  public String getPath() {
    throw new UnsupportedOperationException("Paged blocks do not have a file path");
  }
}
