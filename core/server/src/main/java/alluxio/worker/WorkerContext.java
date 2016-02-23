/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.Configuration;
import alluxio.wire.WorkerNetAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link WorkerContext} object stores {@link Configuration}.
 */
@NotThreadSafe
public final class WorkerContext {
  private WorkerContext() {} // to prevent initialization

  /**
   * The static configuration object. There is only one {@link Configuration} object shared within
   * the same worker process.
   */
  private static Configuration sConf = new Configuration();

  /**
   * The {@link WorkerSource} for collecting worker metrics.
   */
  private static WorkerSource sWorkerSource = new WorkerSource();

  /** Net address of this worker. */
  private static WorkerNetAddress sNetAddress;

  /**
   * Returns the one and only static {@link Configuration} object which is shared among all classes
   * within the worker process.
   *
   * @return the Alluxio configuration for the worker process
   */
  public static Configuration getConf() {
    return sConf;
  }

  /**
   * Returns the one and only static {@link WorkerSource} object which is shared among all classes
   * within the worker process.
   *
   * @return the {@link WorkerSource} for the worker process
   */
  public static WorkerSource getWorkerSource() {
    return sWorkerSource;
  }

  /**
   * @return {@link WorkerNetAddress} object of this worker
   */
  public static WorkerNetAddress getNetAddress() {
    return sNetAddress;
  }

  /**
   * Sets {@link WorkerNetAddress} object of this worker.
   *
   * @param netAddress {@link WorkerNetAddress} object of this worker
   */
  public static void setWorkerNetAddress(WorkerNetAddress netAddress) {
    sNetAddress = netAddress;
  }

  /**
   * Resets the worker context, for test only.
   * TODO(binfan): consider a better way to mock test configuration
   */
  public static void reset() {
    reset(new Configuration());
  }

  /**
   * Resets the worker context, for test only.
   * TODO(binfan): consider a better way to mock test configuration
   *
   * @param conf the configuration for Alluxio
   */
  public static void reset(Configuration conf) {
    sConf = conf;
  }
}
