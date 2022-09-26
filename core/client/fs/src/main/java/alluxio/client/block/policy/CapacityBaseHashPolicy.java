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

package alluxio.client.block.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.WorkerNetAddress;

import org.apache.commons.codec.digest.MurmurHash3;

import java.util.Optional;
import java.util.TreeMap;

/**
 * A policy that pseudo-randomly distributes blocks between workers according to their capacity,
 * so that workers with more capacity have a higher chance of being chosen.
 * The difference between this policy and {@link CapacityBaseRandomPolicy} is that this policy
 * uses the hashed block ID as the index to choose the target worker, so that the same block is
 * always routed to the same worker.
 * {@link MurmurHash3} is used as the hashing function to simulate a uniformly distributed random
 * source.
 *
 * @see  CapacityBaseRandomPolicy
 */
public class CapacityBaseHashPolicy implements BlockLocationPolicy {
  /**
   * Constructor required by
   * {@link BlockLocationPolicy.Factory#create(Class, AlluxioConfiguration)}.
   * @param conf Alluxio configuration
   */
  public CapacityBaseHashPolicy(AlluxioConfiguration conf) {}

  @Override
  public Optional<WorkerNetAddress> getWorker(GetWorkerOptions options) {
    // cumulative distribution function of worker capacities
    TreeMap<Long, BlockWorkerInfo> capacityCdf = new TreeMap<>();
    long totalCapacity = 0;
    for (BlockWorkerInfo workerInfo : options.getBlockWorkerInfos()) {
      if (workerInfo.getCapacityBytes() > 0) {
        capacityCdf.put(totalCapacity, workerInfo);
        totalCapacity += workerInfo.getCapacityBytes();
      }
    }
    if (totalCapacity == 0) {
      return Optional.empty();
    }
    long blockId = options.getBlockInfo().getBlockId();
    // use the hashed value of the block id as the index
    long index = Math.abs(MurmurHash3.hash64(blockId)) % totalCapacity;
    WorkerNetAddress chosen = capacityCdf.floorEntry(index).getValue().getNetAddress();
    return Optional.of(chosen);
  }
}
