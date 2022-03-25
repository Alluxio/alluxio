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

package alluxio.master.job.common;

import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.Status;
import alluxio.master.job.plan.PlanCoordinator;

import com.google.common.base.MoreObjects;

/**
 * Progress information for a distributed command.
 */
public class JobProgress {
  private long mJobId;
  private long mFileCount;
  private long mFileSize;
  private long mLastUpdateMs;
  private String mDescription;
  private String mErrorMsg;
  private String mErrorType;
  private Status mStatus;

  /**
   * Constructor with only jobId.
   * @param jobId
   */
  public JobProgress(long jobId) {
    mJobId = jobId;
    mStatus = Status.CREATED;
  }

  /**
   * Get job id.
   * @return job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * Get file count.
   * @return file count
   */
  public long getFileCount() {
    return mFileCount;
  }

  /**
   * Get file size.
   * @return file size
   */
  public long getFileSize() {
    return mFileSize;
  }

  /**
   * Get status.
   * @return status
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * Get description.
   * @return destring string
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * Get error msg.
   * @return error string
   */
  public String getErrorMsg() {
    return mErrorMsg;
  }

  /**
   * Get error type.
   * @return error type string
   */
  public String getErrorType() {
    return mErrorType;
  }

  /**
   * Get last updated timestamp.
   * @return timestamp
   */
  public long getLastUpdateMs() {
    return mLastUpdateMs;
  }

  /**
   * Update the job progress.
   * @param planCoordinator
   * @param verbose
   * @param fileCount
   * @param fileSize
   */
  public void update(PlanCoordinator planCoordinator, boolean verbose,
                     long fileCount, long fileSize) {
    PlanInfo planInfo = planCoordinator.getPlanInfoWire(verbose);
    mLastUpdateMs = planInfo.getLastUpdated();
    mDescription = planInfo.getDescription();
    mErrorMsg = planInfo.getErrorMessage();
    mErrorType = planInfo.getErrorType();
    mStatus = planInfo.getStatus();
    if (mStatus == Status.COMPLETED) { // update when the job completes
      mFileCount = fileCount;
      mFileSize = fileSize;
    } else { // else just returns 0 when the job is running or failed or canceled.
      mFileCount = 0;
      mFileSize = 0;
    }
  }

  /**
   * toString.
   * @return
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("jobId", mJobId)
            .add("fileCount", mFileCount)
            .add("fileSize", mFileSize)
            .add("lastUpdadte", mLastUpdateMs)
            .add("description", mDescription)
            .add("status", mStatus)
            .add("errorMsg", mErrorMsg)
            .add("errorType", mErrorType)
            .toString();
  }

  /**
   * Convert to protobuf.
   * @return alluxio.grpc.JobProgress
   */
  public alluxio.grpc.JobProgress toProto() {
    alluxio.grpc.JobProgress.Builder builder = alluxio.grpc.JobProgress.newBuilder()
            .setJobId(mJobId)
            .setErrorMsg(mErrorMsg)
            .setErrorType(mErrorType)
            .setFileCount(mFileCount)
            .setFileSize(mFileSize)
            .setLastUpdateMs(mLastUpdateMs)
            .setStatus(mStatus.toProto());
    return builder.build();
  }
}
