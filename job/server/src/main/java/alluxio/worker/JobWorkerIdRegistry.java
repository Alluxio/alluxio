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

package alluxio.worker;

import alluxio.exception.ConnectionFailedException;
import alluxio.master.job.JobMaster;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.job.JobMasterClient;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The single place to get, set, and update job worker id.
 *
 * This class should be the only source of job worker id within the same job worker process.
 */
@NotThreadSafe
public final class JobWorkerIdRegistry {
  /**
   * The default value to initialize job worker id, the job worker id generated by job master will
   * never be the same as this value.
   */
  public static final long INVALID_WORKER_ID = 0;
  private static AtomicLong sWorkerId = new AtomicLong(INVALID_WORKER_ID);

  private JobWorkerIdRegistry() {}

  /**
   * Registers with {@link JobMaster} to get a new job worker id.
   *
   * @param jobMasterClient the job master client to be used for RPC
   * @param workerAddress current worker address
   * @throws IOException when fails to get a new worker id
   * @throws ConnectionFailedException if network connection failed
   */
  public static void registerWorker(JobMasterClient jobMasterClient, WorkerNetAddress workerAddress)
      throws IOException, ConnectionFailedException {
    sWorkerId.set(jobMasterClient.registerWorker(workerAddress));
  }

  /**
   * @return job worker id, 0 is invalid job worker id, representing that the job worker hasn't
   *         been registered with job master
   */
  public static Long getWorkerId() {
    return sWorkerId.get();
  }
}
