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

package alluxio.membership;

import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;

import java.util.Arrays;
import java.util.Optional;

/**
 * Cluster view. A view may be live or a snapshot.
 */
public interface WorkerClusterView extends Iterable<WorkerInfo> {
  Optional<WorkerInfo> getWorkerById(WorkerIdentity workerIdentity);

  /**
   * Narrows the view with a filter.
   *
   * @param filter the filter to filter the view
   * @return filtered view
   * @throws UnsupportedOperationException if the view cannot be filtered with the given filter
   */
  WorkerClusterView filter(ClusterViewFilter filter);

  /**
   * Creates a snapshot of a live view.
   *
   * @return snapshot
   */
  default WorkerClusterView snapshot() {
    return new WorkerClusterSnapshot(this);
  }

  /**
   * Creates a static view of the given workers.
   *
   * @param workers worker in the cluster
   * @return a view of the given workers
   */
  static WorkerClusterView ofWorkers(WorkerInfo... workers) {
    return new WorkerClusterSnapshot(Arrays.stream(workers)::iterator);
  }
}
