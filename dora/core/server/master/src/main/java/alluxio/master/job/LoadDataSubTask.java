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

import alluxio.underfs.UfsStatus;

import com.google.common.annotations.VisibleForTesting;

/**
 * Load data subtask.
 */
public class LoadDataSubTask extends LoadSubTask {
  private final long mVirtualBlockSize;
  private final long mOffset;
  private final long mLength;

  LoadDataSubTask(UfsStatus ufsStatus, long virtualBlockSize, long offset, long length) {
    super(ufsStatus);
    mVirtualBlockSize = virtualBlockSize;
    mOffset = offset;
    int index = mVirtualBlockSize == 0 ? 0 : (int) (offset / virtualBlockSize);
    if (virtualBlockSize == 0) {
      mHashKey = new ConsistentHashShardKey(ufsStatus.getUfsFullPath().toString());
    }
    else {
      mHashKey = new VirtualBlockShardKey(ufsStatus.getUfsFullPath().toString(), index);
    }
    mLength = length;
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  boolean isLoadMetadata() {
    return false;
  }

  @Override
  alluxio.grpc.LoadSubTask toProto() {
    alluxio.grpc.LoadDataSubTask subtask =
        alluxio.grpc.LoadDataSubTask.newBuilder().setOffsetInFile(mOffset).setUfsPath(getUfsPath())
                                    .setLength(getLength()).setUfsStatus(mUfsStatus.toProto())
                                    .build();
    return alluxio.grpc.LoadSubTask.newBuilder().setLoadDataSubtask(subtask).build();
  }

  @Override
  public String asString() {
    return mHashKey.asString();
  }

  /**
   * @return the offset
   */
  @VisibleForTesting
  public long getOffset() {
    return mOffset;
  }

  @Override
  public LoadSubTask copy() {
    return new LoadDataSubTask(mUfsStatus, mVirtualBlockSize, mOffset, mLength);
  }
}
