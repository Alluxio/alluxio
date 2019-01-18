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
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.worker.job.JobMasterClientConfig;

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
    public static JobMasterClient create(JobMasterClientConfig conf) {
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
   * Gets the status of the given job.
   *
   * @param id the job id
   * @return the job information
   */
  JobInfo getStatus(long id) throws IOException;

  /**
   * @return the list of ids of all jobs
   */
  List<Long> list() throws IOException;

  /**
   * Starts a job based on the given configuration.
   *
   * @param jobConfig the job configuration
   * @return the job id
   */
  long run(JobConfig jobConfig) throws IOException;
}
