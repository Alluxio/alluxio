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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An impl of WorkerLocationPolicy.
 *
 * A policy where a file path is matched to worker(s) by a consistenct hashing algorithm.
 * The hash algorithm makes sure the same path maps to the same worker sequence.
 * On top of that, consistent hashing makes sure worker membership changes incur minimal
 * hash changes.
 */
public class KetamaHashPolicy implements WorkerLocationPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(KetamaHashPolicy.class);
  private final KetamaHashProvider mHashProvider;

  /**
   * Constructs a new {@link KetamaHashPolicy}.
   *
   * @param conf the configuration used by the policy
   */
  public KetamaHashPolicy(AlluxioConfiguration conf) {
    LOG.debug("%s is chosen for user worker hash algorithm",
        conf.getString(PropertyKey.USER_WORKER_SELECTION_POLICY));
    mHashProvider = new KetamaHashProvider(100, Constants.SECOND_MS,
        conf.getInt(PropertyKey.USER_KETAMA_HASH_REPLICAS));
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId, int count) throws ResourceExhaustedException {
    if (blockWorkerInfos.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Not enough workers in the cluster %d workers in the cluster but %d required",
          blockWorkerInfos.size(), count));
    }
    mHashProvider.refresh(blockWorkerInfos);
    List<BlockWorkerInfo> workers = mHashProvider.getMultiple(fileId, count);
    if (workers.size() != count) {
      throw new ResourceExhaustedException(String.format(
          "Found %d workers from the hash ring but %d required", blockWorkerInfos.size(), count));
    }
    return workers;
  }
}
