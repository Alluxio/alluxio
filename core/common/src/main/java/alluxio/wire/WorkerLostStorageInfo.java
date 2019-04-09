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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Class to represent a ufs info.
 */
public final class WorkerLostStorageInfo implements Serializable {
  private static final long serialVersionUID = 5774844615334024953L;

  private WorkerNetAddress mWorkerAddress;
  private Map<String, List<String>> mLostStorageOnTiers;

  /**
   * Creates a new instance of {@link WorkerLostStorageInfo}.
   */
  public WorkerLostStorageInfo() {}

  /**
   * @return the Alluxio URI
   */
  public WorkerNetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the mount options
   */
  public Map<String, List<String>> getLostStorageOnTiers() {
    return mLostStorageOnTiers;
  }

  /**
   * @param workerNetAddress the worker net address to set
   * @return the ufs info
   */
  public WorkerLostStorageInfo setWorkerAddress(WorkerNetAddress workerNetAddress) {
    mWorkerAddress = workerNetAddress;
    return this;
  }

  /**
   * @param lostStorageOnTiers the mount options
   * @return the ufs info
   */
  public WorkerLostStorageInfo setLostStorageOnTier(Map<String, List<String>> lostStorageOnTiers) {
    mLostStorageOnTiers = lostStorageOnTiers;
    return this;
  }
}
