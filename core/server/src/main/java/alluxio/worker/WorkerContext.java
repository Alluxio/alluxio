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

  /**
   * @return the {@link WorkerSource} for the worker process
   */
  public static WorkerSource getWorkerSource() {
    return sWorkerSource;
  }
}
