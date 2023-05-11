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

package alluxio.job;

/**
 * A context which contains useful information that can be used when running a task on a job worker.
 *
 * The constructor of this class should always call {@code #super(JobServerContext)} in order to
 * initialize the other variables in the context - namely the filesystem client, context and
 * UfsManager
 */
public class RunTaskContext extends JobServerContext {
  private final long mJobId;
  private final long mTaskId;

  /**
   * Creates an instance of {@link RunTaskContext}.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param jobServerContext the context of the Alluxio job worker
   */
  public RunTaskContext(long jobId, long taskId, JobServerContext jobServerContext) {
    super(jobServerContext);
    mJobId = jobId;
    mTaskId = taskId;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the task id
   */
  public long getTaskId() {
    return mTaskId;
  }
}
