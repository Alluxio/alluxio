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
import alluxio.client.file.FileSystemContext;
import alluxio.wire.WorkerInfo;
import alluxio.worker.dora.WorkerClusterView;

import java.util.List;

public interface WorkerLocationPolicy {
  class Factory {
    public static WorkerLocationPolicy create(FileSystemContext fsContext) {
      int numVirtualNodes = 2000;
      return new ConsistentHashPolicy(numVirtualNodes);
    }
  }

  /**
   * Picks up to {@code count} workers for the given file of ID {@code fileId}.
   *
   * @param workerClusterView view of the current cluster
   * @param fileId file ID
   * @param count number of workers to use
   * @return preferred worker list
   */
  List<BlockWorkerInfo> getPreferredWorkers(WorkerClusterView workerClusterView,
      String fileId, int count);

  /**
   * Creates a {@link WorkerClusterView} that can be used to query the worker location selection
   * via {@link #getPreferredWorkers(WorkerClusterView, String, int)}.
   *
   * @param workers a list of workers in the cluster
   * @return a worker cluster view
   */
  WorkerClusterView createView(Iterable<WorkerInfo> workers);
}
