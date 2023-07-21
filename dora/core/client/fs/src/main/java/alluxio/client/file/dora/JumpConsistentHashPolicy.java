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

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.exception.status.ResourceExhaustedException;

import java.util.List;

/**
 * An impl of WorkerLocationPolicy.
 *
 * A policy where a file path is matched to worker(s) by Jump Consistent Hashing Algorithm.
 * The algorithm is described in this paper:
 * https://arxiv.org/pdf/1406.2294.pdf
 */
public class JumpConsistentHashPolicy implements WorkerLocationPolicy {
  private static final JumpConsistentHashProvider HASH_PROVIDER =
      new JumpConsistentHashProvider(100, Constants.SECOND_MS);

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId, int count) throws ResourceExhaustedException {
    if (blockWorkerInfos.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Not enough workers in the cluster %d workers in the cluster but %d required",
          blockWorkerInfos.size(), count));
    }
    HASH_PROVIDER.refresh(blockWorkerInfos);
    List<BlockWorkerInfo> workers = HASH_PROVIDER.getMultiple(fileId, count);
    if (workers.size() != count) {
      throw new ResourceExhaustedException(String.format(
          "Found %d workers from the hash ring but %d required", blockWorkerInfos.size(), count));
    }
    return workers;
  }
}
