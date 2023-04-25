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
 * A context containing useful parameters for the job master to use when selecting the worker
 * that a job should execute on.
 *
 * The constructor of this class should always call {@code #super(JobServerContext)} in order to
 * initialize the other variables in the context - namely the filesystem client, context and
 * UfsManager
 */
public class SelectExecutorsContext extends JobServerContext {
  private final long mJobId;

  /**
   * Creates a new instance of {@link SelectExecutorsContext}.
   *
   * @param jobId the id of the job
   * @param jobServerContext the context of the Alluxio job master
   */
  public SelectExecutorsContext(long jobId, JobServerContext jobServerContext) {
    super(jobServerContext);
    mJobId = jobId;
  }

  /**
   * @return the job Id
   */
  public long getJobId() {
    return mJobId;
  }
}
