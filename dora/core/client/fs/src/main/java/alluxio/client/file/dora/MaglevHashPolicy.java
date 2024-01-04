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
 * An impl of Maglev Hash policy.
 *
 * A policy where a file path is matched to worker(s) by Jump Consistent Hashing Algorithm.
 * The algorithm is described in this paper:
 * https://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/44824.pdf
 *
 * One thing to note about Maglev hashing is that alluxio.user.maglev.hash.lookup.size
 * needs to be set to a prime number.
 * The bigger the size of the lookup table,
 * the smaller the variance of this hashing algorithm will be.
 * But bigger look up table will consume more time and memory.
 *
 * We strongly recommend using Maglev Hashing for User Worker Selection Policy.
 * Under most situation, it has the minimum time cost,
 * and it is the most uniform and balanced hashing policy.
 *
 */
public class MaglevHashPolicy implements WorkerLocationPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(MaglevHashPolicy.class);
  private final MaglevHashProvider mHashProvider;

  /**
   * Constructor.
   * @param conf Alluxio Configuration
   */
  public MaglevHashPolicy(AlluxioConfiguration conf) {
    LOG.debug("%s is chosen for user worker hash algorithm",
        conf.getString(PropertyKey.USER_WORKER_SELECTION_POLICY));
    int lookupSize = conf.getInt(PropertyKey.USER_MAGLEV_HASH_LOOKUP_SIZE);
    mHashProvider = new MaglevHashProvider(100, Constants.SECOND_MS, lookupSize);
    if (!isPrime(lookupSize)) {
      System.out.println("The number of alluxio.user.maglev.hash.lookup.size "
          + "must be a prime number!");
    }
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

  private boolean isPrime(int n) {
    if (n <= 1) {
      return false;
    }
    for (int i = 2; i * i <= n; ++i) {
      if (n % i == 0) {
        return false;
      }
    }
    return true;
  }
}
