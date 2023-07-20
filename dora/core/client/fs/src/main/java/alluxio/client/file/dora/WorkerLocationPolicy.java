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
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.util.CommonUtils;

import java.util.List;

/**
 * Interface for determining the Alluxio worker location to serve a read or write request.
 *
 * A policy MUST have the property of being deterministic, because distributed clients must
 * be able to resolve the same path to the same worker(s) for the file.
 * Imagine a totally random policy which resolves a path to a random worker in the cluster.
 * The first reader of path /a will resolve to a random worker in the cluster and leave a cache
 * there. A subsequent reader will resolve to another random worker and the chance of a cache-hit
 * is very low! Worse still, that subsequent reader may produce another cache replica.
 * This random policy wastes cache space and provides terrible cache hit rate.
 */
public interface WorkerLocationPolicy {
  /**
   * Find a specified number of workers following the logic defined by the policy.
   * This method should return exactly #{count} different workers, no more no less.
   * If the specified number of workers cannot be found, this method will throw
   * a {@link ResourceExhaustedException}.
   *
   * We want the semantics here to be explicit when the requirement cannot be satisfied.
   * So the caller should define its own logic handling the exception and finding backups.
   *
   * @param blockWorkerInfos
   * @param fileId
   * @param count
   * @return a list of preferred workers
   * @throws ResourceExhaustedException if unable to return exactly #{count} workers
   */
  List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId, int count) throws ResourceExhaustedException;

  /**
   * The factory for the {@link WorkerLocationPolicy}.
   */
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Factory for creating {@link WorkerLocationPolicy}.
     *
     * @param conf Alluxio configuration
     * @return a new instance of {@link WorkerLocationPolicy}
     */
    public static WorkerLocationPolicy create(AlluxioConfiguration conf) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.getClass(PropertyKey.USER_WORKER_SELECTION_POLICY),
            new Class[] {AlluxioConfiguration.class}, new Object[] {conf});
      } catch (ClassCastException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
