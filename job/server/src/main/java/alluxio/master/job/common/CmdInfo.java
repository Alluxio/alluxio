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

import alluxio.grpc.OperationType;
import alluxio.job.wire.JobSource;
import alluxio.master.job.tracker.CmdRunAttempt;

import com.google.common.base.MoreObjects;
import org.apache.commons.compress.utils.Lists;

import java.util.List;

/**
 * A class representation for Command Information.
 */
public class CmdInfo {
  private final long mJobControlId;
  private final List<CmdRunAttempt> mCmdRunAttempt;
  private final OperationType mOperationType;
  private final JobSource mJobSource;
  private final long mJobSubmissionTime;
  private final List<String> mFilePath;

  /**
   * Constructor for CmdInfo class.
   * @param jobControlId control id
   * @param operationType operation type
   * @param jobSource where the job comes from
   * @param jobSubmissionTime job submission time
   * @param filePath file path
   */
  public CmdInfo(long jobControlId,
      OperationType operationType, JobSource jobSource,
      long jobSubmissionTime, List<String> filePath) {
    mJobControlId = jobControlId;
    mOperationType = operationType;
    mJobSource = jobSource;
    mJobSubmissionTime = jobSubmissionTime;
    mFilePath = filePath;
    mCmdRunAttempt = Lists.newArrayList();
  }

  /**
   * Add the CmdRunAttempt.
   * @param attempt CmdRunAttempt
   */
  public void addCmdRunAttempt(CmdRunAttempt attempt) {
    mCmdRunAttempt.add(attempt);
  }

  /** Get CmdRunAttempt.
   * @return list of attempt
   */
  public List<CmdRunAttempt> getCmdRunAttempt() {
    return mCmdRunAttempt;
  }

  /** Get job control Id.
   * @return job control id
   */
  public long getJobControlId() {
    return mJobControlId;
  }

  /** Get operation type.
   * @return operation type
   */
  public OperationType getOperationType() {
    return mOperationType;
  }

  /** Get job source.
   * @return job source
   */
  public JobSource getJobSource() {
    return mJobSource;
  }

  /** Get submission time.
   * @return timestamp
   */
  public long getJobSubmissionTime() {
    return mJobSubmissionTime;
  }

  /** Get file path.
   * @return list of paths
   */
  public List<String> getFilePath() {
    return mFilePath;
  }

  /**
   * tostring.
   * @return cmd info
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("jobControlId", mJobControlId)
            .add("operationType", mOperationType)
            .add("jobSource", mJobSource)
            .add("submission time", mJobSubmissionTime)
            .add("attempts", mCmdRunAttempt)
            .toString();
  }
}
