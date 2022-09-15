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

/**
 * Metadata of a paged block.
 */
public class PagedBlockMeta implements BlockMeta {
  private final long mBlockId;
  private final long mBlockSize;
  private final PagedBlockStoreDir mDir;

  /**
   * @param blockId
   * @param blockSize
   * @param dir the directory that stores the pages of this block
   */
  public PagedBlockMeta(long blockId, long blockSize, PagedBlockStoreDir dir) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mDir = dir;
  }

  @Override
  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public BlockStoreLocation getBlockLocation() {
    return mDir.getLocation();
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  @Override
  public StorageDir getParentDir() {
    // todo(bowen): make PagedBlockStoreDir implement StorageDir?
    throw new UnsupportedOperationException("getParentDir");
  }

  /**
   * @return dir
   */
  public PagedBlockStoreDir getDir() {
    return mDir;
  }

  @Override
  public String getPath() {
    // todo(bowen): return path of the directory for block?
    throw new UnsupportedOperationException("getPath is not supported for a paged block as the "
        + "block does not have a single file representation");
  }
}
