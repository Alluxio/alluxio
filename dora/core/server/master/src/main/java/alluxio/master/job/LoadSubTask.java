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

import alluxio.common.ShardKey;
import alluxio.grpc.LoadFailure;
import alluxio.underfs.UfsStatus;
import alluxio.wire.WorkerInfo;

/**
 * Load sub task. It's either load metadata or load data.
 */
public abstract class LoadSubTask implements ShardKey {
  protected UfsStatus mUfsStatus;
  protected ShardKey mHashKey;
  private WorkerInfo mWorkerInfo;

  LoadSubTask(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
  }

  /**
   * @return the shallow copy of the subtask
   */
  public abstract LoadSubTask copy();

  /**
   * @return the length
   */
  abstract long getLength();

  /**
   * @return the ufs path
   */
  public String getUfsPath() {
    return mUfsStatus.getUfsFullPath().toString();
  }

  /**
   * @return whether it's load metadata task or it's load data task
   */
  abstract boolean isLoadMetadata();

  abstract alluxio.grpc.LoadSubTask toProto();

  /**
   * @return the worker to run the task
   */
  public WorkerInfo getWorkerInfo() {
    return mWorkerInfo;
  }

  /**
   * @param worker the worker info
   * @return the subtask
   */
  public LoadSubTask setWorkerInfo(WorkerInfo worker) {
    mWorkerInfo = worker;
    return this;
  }

  /**
   * @param loadFailure      the subtask failure from worker
   * @param virtualBlockSize the virtual block size
   * @return the subtask
   */
  public static LoadSubTask from(LoadFailure loadFailure, long virtualBlockSize) {
    alluxio.grpc.LoadSubTask failure = loadFailure.getSubtask();
    if (failure.hasUfsStatus()) {
      return new LoadMetadataSubTask(UfsStatus.fromProto(failure.getUfsStatus()), virtualBlockSize);
    }
    else {
      UfsStatus status = UfsStatus.fromProto(failure.getBlock().getUfsStatus());
      return new LoadDataSubTask(status, virtualBlockSize, failure.getBlock().getOffsetInFile(),
          failure.getBlock().getLength());
    }
  }
}
