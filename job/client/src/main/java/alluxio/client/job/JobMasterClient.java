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

package alluxio.client.job;

import alluxio.Client;
import alluxio.grpc.ListAllPOptions;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.worker.job.JobMasterClientContext;

import java.io.IOException;
import java.util.List;

/**
 * Interface for job service clients to communicate with the job master.
 */
public interface JobMasterClient extends Client {

  /**
   * Factory for {@link JobMasterClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link JobMasterClient}.
     *
     * @param conf job master client configuration
     * @return a new {@link JobMasterClient} instance
     */
    public static JobMasterClient create(JobMasterClientContext conf) {
      return new RetryHandlingJobMasterClient(conf);
    }
  }

  /**
   * Cancels the given job.
   *
   * @param id the job id
   */
  void cancel(long id) throws IOException;

  /**
   * Gets the status of the given job id.
   *
   * @param id the job id
   * @return the job information
   */
  JobInfo getJobStatus(long id) throws IOException;

  /**
   * Gets detailed status of the given job id.
   *
   * @param id the job id
   * @return the detailed job information
   */
  JobInfo getJobStatusDetailed(long id) throws IOException;

  /**
   * Gets the job service summary.
   *
   * @return the job service summary
   */
  JobServiceSummary getJobServiceSummary() throws IOException;

  /**
   * @return list all job ids
   */
  default List<Long> list() throws IOException {
    return list(ListAllPOptions.getDefaultInstance());
  }

  /**
   * @param option list options
   * @return the list of ids of all jobs
   */
  List<Long> list(ListAllPOptions option) throws IOException;

  /**
   * @return the list of all jobInfos
   */
  List<JobInfo> listDetailed() throws IOException;

  /**
   * Starts a plan based on the given configuration.
   *
   * @param jobConfig the job configuration
   * @return the plan id
   */
  long run(JobConfig jobConfig) throws IOException;

  /**
   * Gets all worker health.
   * @return list of all worker health information
   */
  List<JobWorkerHealth> getAllWorkerHealth() throws IOException;
}
