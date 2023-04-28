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

package alluxio.master;

/***
 * The worker state maintained by master.
 */
public enum WorkerState {
<<<<<<< HEAD:core/server/master/src/main/java/alluxio/master/block/meta/WorkerState.java
  LIVE("In Service"),
  LOST("Out of Service"),
  DECOMMISSIONED("Decommissioned"),
  DISABLED("Disabled");
||||||| parent of 26257e6f35 (Make capacity command show worker state):core/server/master/src/main/java/alluxio/master/block/meta/WorkerState.java
  LIVE("In Service"),
  LOST("Out of Service"),
  DECOMMISSIONED("Decommissioned");
=======
  LIVE("ACTIVE"),
  LOST("LOST"),
  DECOMMISSIONED("Decommissioned"),
  DISABLED("Disabled");
>>>>>>> 26257e6f35 (Make capacity command show worker state):core/common/src/main/java/alluxio/master/WorkerState.java
  private final String mState;

  WorkerState(String s) {
    mState = s;
  }

  @Override
  public String toString() {
    return mState;
  }
}
