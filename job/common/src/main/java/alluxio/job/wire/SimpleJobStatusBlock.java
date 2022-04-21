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

package alluxio.job.wire;

import com.google.common.base.Objects;

/**
 * Class for simple job status information.
 */
public class SimpleJobStatusBlock {
  private final long mJobId;
  private final Status mStatus;
  private final String mFilesPathString;
  private final String mFilesPathFailed;

  /**
   * Constructor.
   * @param jobId
   * @param status
   * @param filePath
   * @param fileFailed
   */
  public SimpleJobStatusBlock(long jobId, Status status, String filePath, String fileFailed) {
    mJobId = jobId;
    mStatus = status;
    mFilesPathString = filePath;
    mFilesPathFailed = fileFailed;
  }

  /**
   * Get job id.
   * @return job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * Get status of the job.
   * @return status information
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * Get file path of the job.
   * @return file path s tring
   */
  public String getFilePath() {
    return mFilesPathString;
  }

  /**
   * Get failed file path of the job.
   * @return file path s tring
   */
  public String getFilesPathFailed() {
    return mFilesPathFailed;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof SimpleJobStatusBlock)) {
      return false;
    }
    SimpleJobStatusBlock that = (SimpleJobStatusBlock) o;
    return Objects.equal(mJobId, that.mJobId)
            && Objects.equal(mStatus, that.mStatus)
            && Objects.equal(mFilesPathString, that.mFilesPathString)
            && Objects.equal(mFilesPathFailed, that.mFilesPathFailed);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mStatus, mFilesPathString, mFilesPathFailed);
  }
}
