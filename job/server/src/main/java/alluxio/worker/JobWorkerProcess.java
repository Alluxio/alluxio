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

import alluxio.Process;
import alluxio.wire.WorkerNetAddress;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job worker in the Alluxio system.
 */
public interface JobWorkerProcess extends Process {
  /**
   * Factory for creating {@link JobWorkerProcess}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link JobWorkerProcess}
     */
    public static JobWorkerProcess create() {
      return new AlluxioJobWorkerProcess();
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @return the connect information for this worker
   */
  WorkerNetAddress getAddress();

  /**
   * @return this worker's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the worker in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the worker in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  InetSocketAddress getWebAddress();
}
