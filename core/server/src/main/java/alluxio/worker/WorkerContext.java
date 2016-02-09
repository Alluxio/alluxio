/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
