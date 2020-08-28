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

package alluxio.worker.job;

import alluxio.Client;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.grpc.JobCommand;
import alluxio.grpc.JobInfo;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

/**
 * Interface for job service workers to communicate with the job master.
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
   * Returns a worker id for a workers net address.
   *
   * @param address the net address to get a worker id for
   * @return a worker id
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  long registerWorker(final WorkerNetAddress address) throws IOException, ConnectionFailedException;

  /**
   * Periodic heartbeats to update the tasks' status from a worker, and returns the commands.
   *
   * @param jobWorkerHealth the job worker info
   * @param taskInfoList the list of the task information
   * @return the commands issued to the worker
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  List<JobCommand> heartbeat(final JobWorkerHealth jobWorkerHealth,
      final List<JobInfo> taskInfoList) throws AlluxioException, IOException;
}
