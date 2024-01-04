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
import alluxio.membership.WorkerClusterView;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerState;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * An impl of Jump Consistent Hash Policy.
 *
 * A policy where a file path is matched to worker(s) by Jump Consistent Hashing Algorithm.
 * The algorithm is described in this paper:
 * https://arxiv.org/pdf/1406.2294.pdf
 *
 * The disadvantage of this algorithm is that
 * buckets can only be inserted and deleted at the head and tail of the worker list
 * to maintain hash consistency,
 * that is, nodes can only be added and deleted at the head and tail of the worker list.
 */
public class JumpHashPolicy implements WorkerLocationPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(JumpHashPolicy.class);
  private final JumpHashProvider mHashProvider;

  /**
   * Constructs a new {@link JumpHashPolicy}.
   *
   * @param conf the configuration used by the policy
   */
  public JumpHashPolicy(AlluxioConfiguration conf) {
    LOG.debug("%s is chosen for user worker hash algorithm",
        conf.getString(PropertyKey.USER_WORKER_SELECTION_POLICY));
    mHashProvider = new JumpHashProvider(100, Constants.SECOND_MS);
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(WorkerClusterView workerClusterView,
      String fileId, int count) throws ResourceExhaustedException {
    if (workerClusterView.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Not enough workers in the cluster %d workers in the cluster but %d required",
          workerClusterView.size(), count));
    }
    Set<WorkerIdentity> workerIdentities = workerClusterView.workerIds();
    mHashProvider.refresh(workerIdentities);
    List<WorkerIdentity> workers = mHashProvider.getMultiple(fileId, count);
    if (workers.size() != count) {
      throw new ResourceExhaustedException(String.format(
          "Found %d workers from the hash ring but %d required", workers.size(), count));
    }
    ImmutableList.Builder<BlockWorkerInfo> builder = ImmutableList.builder();
    for (WorkerIdentity worker : workers) {
      Optional<WorkerInfo> optionalWorkerInfo = workerClusterView.getWorkerById(worker);
      final WorkerInfo workerInfo;
      if (optionalWorkerInfo.isPresent()) {
        workerInfo = optionalWorkerInfo.get();
      } else {
        // the worker returned by the policy does not exist in the cluster view
        // supplied by the client.
        // this can happen when the membership changes and some callers fail to update
        // to the latest worker cluster view.
        // in this case, just skip this worker
        LOG.debug("Inconsistency between caller's view of cluster and that of "
                + "the consistent hash policy's: worker {} selected by policy does not exist in "
                + "caller's view {}. Skipping this worker.",
            worker, workerClusterView);
        continue;
      }

      BlockWorkerInfo blockWorkerInfo = new BlockWorkerInfo(
          worker, workerInfo.getAddress(), workerInfo.getCapacityBytes(),
          workerInfo.getUsedBytes(), workerInfo.getState() == WorkerState.LIVE
      );
      builder.add(blockWorkerInfo);
    }
    List<BlockWorkerInfo> infos = builder.build();
    return infos;
  }
}
