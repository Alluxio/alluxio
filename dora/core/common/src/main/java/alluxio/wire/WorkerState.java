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

package alluxio.wire;

/***
 * The worker state maintained by master.
 */
public enum WorkerState {
  LIVE("LIVE"),
  LOST("LOST"),
  DECOMMISSIONED("Decommissioned"),
  DISABLED("Disabled"),
  // an unknown worker which is not recognized by the cluster membership manager,
  // e.g. a worker before it registers to the manager
  UNRECOGNIZED("UNRECOGNIZED");

  private final String mState;

  WorkerState(String s) {
    mState = s;
  }

  /**
   * Converts from string to worker state.
   *
   * @param workerState string representation of worker state
   * @return worker state
   * @throws IllegalArgumentException if the state is unknown
   */
  public static WorkerState of(String workerState) throws IllegalArgumentException {
    switch (workerState) {
      case "LIVE":
        return LIVE;
      case "LOST":
        return LOST;
      case "Decommissioned":
        return DECOMMISSIONED;
      case "Disabled":
        return DISABLED;
      case "UNRECOGNIZED":
        return UNRECOGNIZED;
      default:
        throw new IllegalArgumentException("Unknown worker state: " + workerState);
    }
  }

  @Override
  public String toString() {
    return mState;
  }
}
