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
import alluxio.worker.block.meta.TempBlockMeta;

/**
 * Metadata of blocks for paged store.
 */
public class PagedBlockMeta implements TempBlockMeta, BlockMeta {
  private long mSize;
  private long mBlockId;
  private String mPath;

  @Override
  public long getBlockSize() {
    return mSize;
  }

  @Override
  public String getPath() {
    return mPath;
  }

  @Override
  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public BlockStoreLocation getBlockLocation() {
    return null;
  }

  @Override
  public StorageDir getParentDir() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCommitPath() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getSessionId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBlockSize(long newSize) {
    mSize = newSize;
  }
}
