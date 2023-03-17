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

package alluxio.client.block;

import alluxio.collections.Pair;
import alluxio.wire.WorkerInfo;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * A data class to persist aggregated worker info from all masters, including standby masters.
 * Used when worker all master registration feature is enabled.
 */
public class AllMastersWorkerInfo {
  private final Map<Long, InetSocketAddress> mWorkerIdAddressMap;
  private final List<InetSocketAddress> mMasterAddresses;
  private final InetSocketAddress mPrimaryMasterAddress;
  private final List<WorkerInfo> mPrimaryMasterWorkerInfo;
  private final Map<Long, List<Pair<InetSocketAddress, WorkerInfo>>> mWorkerIdInfoMap;

  /**
   * @param workerIdAddressMap the worker id to address map
   * @param masterAddresses the master addresses
   * @param primaryMasterAddress the primary master address
   * @param primaryMasterWorkerInfo the worker info of the primary master
   * @param workerIdInfoMap the worker id to worker info map
   */
  public AllMastersWorkerInfo(
      Map<Long, InetSocketAddress> workerIdAddressMap,
      List<InetSocketAddress> masterAddresses,
      InetSocketAddress primaryMasterAddress,
      List<WorkerInfo> primaryMasterWorkerInfo,
      Map<Long, List<Pair<InetSocketAddress, WorkerInfo>>> workerIdInfoMap) {
    mWorkerIdAddressMap = workerIdAddressMap;
    mMasterAddresses = masterAddresses;
    mPrimaryMasterAddress = primaryMasterAddress;
    mPrimaryMasterWorkerInfo = primaryMasterWorkerInfo;
    mWorkerIdInfoMap = workerIdInfoMap;
  }

  /**
   * @return the worker id to worker address map
   */
  public Map<Long, InetSocketAddress> getWorkerIdAddressMap() {
    return mWorkerIdAddressMap;
  }

  /**
   * @return the master addresses for all masters
   */
  public List<InetSocketAddress> getMasterAddresses() {
    return mMasterAddresses;
  }

  /**
   * @return the primary master address
   */
  public InetSocketAddress getPrimaryMasterAddress() {
    return mPrimaryMasterAddress;
  }

  /**
   * @return the worker info for all workers from the primary master
   */
  public List<WorkerInfo> getPrimaryMasterWorkerInfo() {
    return mPrimaryMasterWorkerInfo;
  }

  /**
   * @return a map which keys are the worker ids and values are lists of pairs,
   * the first element in the pair is the master address and the second element is
   * the worker info for such worker id gotten from the master with such master address.
   */
  public Map<Long, List<Pair<InetSocketAddress, WorkerInfo>>> getWorkerIdInfoMap() {
    return mWorkerIdInfoMap;
  }
}
