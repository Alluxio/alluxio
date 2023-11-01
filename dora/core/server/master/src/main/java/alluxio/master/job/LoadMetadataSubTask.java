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

/**
 * Load metadata subtask.
 */
public class LoadMetadataSubTask extends LoadSubTask {

  private final long mVirtualBlockSize;

  LoadMetadataSubTask(UfsStatus ufsStatus, long virtualBlockSize) {
    super(ufsStatus);
    mVirtualBlockSize = virtualBlockSize;
    if (virtualBlockSize == 0) {
      mHashKey = new ConsistentHashShardKey(ufsStatus.getUfsFullPath().toString());
    }
    else {
      mHashKey = new VirtualBlockShardKey(ufsStatus.getUfsFullPath().toString(), 0);
    }
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  boolean isLoadMetadata() {
    return true;
  }

  @Override
  alluxio.grpc.LoadSubTask toProto() {
    return alluxio.grpc.LoadSubTask.newBuilder().setLoadMetadataSubtask(
                      alluxio.grpc.LoadMetadataSubTask.newBuilder().setUfsStatus(
                          mUfsStatus.toProto()).build()).build();
  }

  @Override
  public String asString() {
    return mHashKey.asString();
  }

  @Override
  public LoadSubTask copy() {
    return new LoadMetadataSubTask(mUfsStatus, mVirtualBlockSize);
  }
}
