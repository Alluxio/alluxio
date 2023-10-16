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

package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * A singleton element used in RemoteOnlyPolicy.
 */
public class WorkerInfoListSingleton {
  private static WorkerInfoListSingleton sInstance;

  private List<BlockWorkerInfo> mWorkerInfoList;

  /**
   * A Constructor, set as private to avoid concurrent modification.
   */
  private WorkerInfoListSingleton() {
    mWorkerInfoList = new ArrayList<>();
  }

  /**
   * A synchronized function to get the singleton object.
   *
   * @return The singleton object; can be fetched by one thread only
   */
  public static synchronized WorkerInfoListSingleton getInstance() {
    if (sInstance == null) {
      sInstance = new WorkerInfoListSingleton();
    }
    return sInstance;
  }

  /**
   * Check if the worker info list is empty.
   *
   * @return True if the list is empty; false otherwise
   */
  public boolean isEmpty() {
    return mWorkerInfoList.isEmpty();
  }

  /**
   * A synchronized function to initialize the worker list.
   *
   * @param workerList The worker list to initialize
   */
  public synchronized void initWorkerList(List<BlockWorkerInfo> workerList) {
    mWorkerInfoList = workerList;
  }

  /**
   * Get the list of workers for the policy.
   *
   * @return The stored worker list
   */
  public List<BlockWorkerInfo> getWorkerList() {
    return mWorkerInfoList;
  }

  /**
   * A synchronized function to round-robin in worker list.
   * Specifically, it gets the first element, remove it and add it to the end of the list.
   */
  public synchronized void roulette() {
    if (!mWorkerInfoList.isEmpty()) {
      BlockWorkerInfo firstWorker = mWorkerInfoList.remove(0);
      mWorkerInfoList.add(firstWorker);
    }
  }
}
