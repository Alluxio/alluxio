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

package alluxio.master.job;

import alluxio.grpc.Block;
import alluxio.underfs.UfsStatus;

/**
 * Load data subtask.
 */
public class LoadDataSubTask extends LoadSubTask {
  private final long mVirtualBlockSize;
  private final long mOffset;

  LoadDataSubTask(UfsStatus ufsStatus, long virtualBlockSize, long offset) {
    super(ufsStatus);
    mVirtualBlockSize = virtualBlockSize;
    mOffset = offset;
    int index = mVirtualBlockSize == 0 ? 0 : (int) (offset / virtualBlockSize);
    mHashKey = new VirtualBlockShardKey(ufsStatus.getUfsFullPath().toString(), index);
  }

  @Override
  public long getLength() {
    if (mVirtualBlockSize == 0) {
      return mUfsStatus.asUfsFileStatus().getContentLength();
    }
    long leftover = mUfsStatus.asUfsFileStatus().getContentLength() - mOffset;
    return Math.min(leftover, mVirtualBlockSize);
  }

  @Override
  boolean isLoadMetadata() {
    return false;
  }

  @Override
  alluxio.grpc.LoadSubTask toProto() {
    Block block =
        Block.newBuilder().setOffsetInFile(mOffset).setUfsPath(getUfsPath()).setLength(getLength())
             .setUfsStatus(mUfsStatus.toProto()).build();
    return alluxio.grpc.LoadSubTask.newBuilder().setBlock(block).build();
  }

  @Override
  public String asString() {
    return mHashKey.asString();
  }
}
