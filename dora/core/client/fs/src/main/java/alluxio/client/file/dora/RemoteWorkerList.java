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
import alluxio.conf.AlluxioConfiguration;
import alluxio.membership.WorkerClusterView;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.wire.WorkerState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This object embeds a list of all remote workers in the cluster.
 * This object maintains an order on all remote workers by its hostname.
 * The order is heuristic so it can be generated anywhere in the cluster.
 */
public class RemoteWorkerList {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteWorkerList.class);
  private static RemoteWorkerList sInstance;

  private final List<WorkerInfo> mRemoteWorkers;
  private int mNextIndex = 0;

  private RemoteWorkerList(WorkerClusterView blockWorkerInfos, AlluxioConfiguration conf) {
    String userHostname = NetworkAddressUtils.getClientHostName(conf);
    mRemoteWorkers = new ArrayList<>();

    for (WorkerInfo worker : blockWorkerInfos) {
      WorkerNetAddress workerAddr = worker.getAddress();
      if (workerAddr == null) {
        continue;
      }
      // Only a plain string match is performed on hostname
      // If one is IP and the other is hostname, a false positive will be returned
      if (!userHostname.equals(workerAddr.getHost())) {
        mRemoteWorkers.add(worker);
      }
    }

    mRemoteWorkers.sort(Comparator.comparing(a -> (a.getAddress().getHost())));
    LOG.debug("{} remote workers found on {}", mRemoteWorkers.size(), userHostname);
  }

  /**
   * A synchronized function to get the singleton object.
   *
   * @param blockWorkerInfos list of all worker infos
   * @param conf             Alluxio configuration
   * @return The singleton object; can be fetched by one thread only
   */
  public static synchronized RemoteWorkerList getInstance(
      WorkerClusterView blockWorkerInfos, AlluxioConfiguration conf) {
    if (sInstance == null) {
      sInstance = new RemoteWorkerList(blockWorkerInfos, conf);
    }
    return sInstance;
  }

  /**
   * A function to find the next worker in the remote worker list.
   *
   * @return the next worker
   */
  public synchronized BlockWorkerInfo findNextWorker() {
    WorkerInfo nextWorker = mRemoteWorkers.get(mNextIndex);
    mNextIndex++;
    if (mNextIndex >= mRemoteWorkers.size()) {
      mNextIndex -= mRemoteWorkers.size();
    }
    return new BlockWorkerInfo(nextWorker.getIdentity(),
        nextWorker.getAddress(),
        nextWorker.getCapacityBytes(),
        nextWorker.getUsedBytes(),
        nextWorker.getState() == WorkerState.LIVE);
  }
}
