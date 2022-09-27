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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import org.apache.commons.codec.digest.MurmurHash3;

import java.util.Comparator;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

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
    AtomicLong totalCapacity = new AtomicLong(0);
    Streams.stream(options.getBlockWorkerInfos())
        // sort by hostname to guarantee two workers with the same capacity has a defined order
        .sorted(Comparator.comparing(w -> w.getNetAddress().getHost()))
        .forEach(workerInfo -> {
          if (workerInfo.getCapacityBytes() > 0) {
            capacityCdf.put(totalCapacity.get(), workerInfo);
            totalCapacity.getAndAdd(workerInfo.getCapacityBytes());
          }
        });
    if (totalCapacity.get() == 0) {
      return Optional.empty();
    }
    long blockId = options.getBlockInfo().getBlockId();
    // use the hashed value of the block id as the index
    long index = randomInCapacity(blockId, totalCapacity.get());
    WorkerNetAddress chosen = capacityCdf.floorEntry(index).getValue().getNetAddress();
    return Optional.of(chosen);
  }

  @VisibleForTesting
  protected long randomInCapacity(long blockId, long totalCapacity) {
    return Math.abs(MurmurHash3.hash64(blockId)) % totalCapacity;
  }
}
