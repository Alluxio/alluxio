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

import alluxio.wire.WorkerNetAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A singleton for storing shared worker state.
 */
@NotThreadSafe
public final class WorkerContext {
  private WorkerContext() {} // to prevent initialization

  /**
   * The {@link WorkerSource} for collecting worker metrics.
   */
  private static WorkerSource sWorkerSource = new WorkerSource();

  /** Net address of this worker. */
  private static WorkerNetAddress sNetAddress;

  /**
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
   * @param netAddress {@link WorkerNetAddress} object of this worker
   */
  public static void setWorkerNetAddress(WorkerNetAddress netAddress) {
    sNetAddress = netAddress;
  }

  /**
   * Resets the worker context.
   */
  public static void reset() {
    sWorkerSource = new WorkerSource();
  }
}
