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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.Configuration;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class CapacityBaseHashPolicyTest {

  private static final CapacityBaseHashPolicy POLICY =
      new CapacityBaseHashPolicy(Configuration.global());

  /**
   * Tests that the probability a worker is chosen is linear to its capacity over total capacity
   * of all workers.
   */
  @Test
  public void linearDistribution() {
    final long capacityUpperBound = 1000;
    final int numWorkers = 100;
    final int numTrials = 100000;
    final List<Long> capacities =
        LongStream.generate(() -> ThreadLocalRandom.current().nextLong(capacityUpperBound))
            .limit(numWorkers).boxed().collect(Collectors.toList());
    final long totalCapacity = capacities.stream().reduce(0L, Long::sum);

    ImmutableMap.Builder<WorkerNetAddress, BlockWorkerInfo> workersBuilder = ImmutableMap.builder();
    for (int i = 0; i < numWorkers; i++) {
      // used bytes shouldn't matter in case of CapacityBaseHashPolicy; random number does not
      // affect the outcome of the policy
      long randomUsedBytes = ThreadLocalRandom.current().nextLong();
      WorkerNetAddress addr = new WorkerNetAddress().setHost(String.valueOf(i));
      BlockWorkerInfo workerInfo = new BlockWorkerInfo(addr, capacities.get(i), randomUsedBytes);
      workersBuilder.put(addr, workerInfo);
    }
    Map<WorkerNetAddress, BlockWorkerInfo> workers = workersBuilder.build();

    BlockInfo blockInfo = new BlockInfo();
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockInfo(blockInfo)
        .setBlockWorkerInfos(ImmutableList.copyOf(workers.values()));
    // worker to number of hits map
    Map<WorkerNetAddress, Long> hits = new HashMap<>();
    for (int i = 0; i < numTrials; i++) {
      blockInfo.setBlockId(ThreadLocalRandom.current().nextLong());
      Optional<WorkerNetAddress> chosen = POLICY.getWorker(options);
      assertTrue(chosen.isPresent());
      hits.computeIfPresent(chosen.get(), (k, v) -> v + 1);
      hits.putIfAbsent(chosen.get(), 1L);
    }
    // the chance that workers of a particular capacity are chosen converges to
    // the ratio of their capacity over total capacity, as the number of trials increases
    final double confidence = 0.01;
    for (Map.Entry<WorkerNetAddress, Long> entry : hits.entrySet()) {
      long capacity = workers.get(entry.getKey()).getCapacityBytes();
      double capacityRatio = capacity * 1.0 / totalCapacity;
      double hitRatio = entry.getValue() * 1.0 / numTrials;
      assertTrue(Math.abs(capacityRatio - hitRatio) < confidence);
    }
  }

  /**
   * Tests that the outcome of the policy is deterministic.
   */
  @Test
  public void deterministicChoice() {
    List<BlockWorkerInfo> workerInfos = generateBlockWorkerInfos(100, 1);
    BlockInfo blockInfo = new BlockInfo().setBlockId(1);
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockInfo(blockInfo)
        .setBlockWorkerInfos(workerInfos);
    WorkerNetAddress chosen = POLICY.getWorker(options).get();
    for (int i = 0; i < 10000; i++) {
      Optional<WorkerNetAddress> workerInfo = POLICY.getWorker(options);
      assertTrue(workerInfo.isPresent());
      assertEquals(chosen, workerInfo.get());
    }
  }

  @Test
  public void zeroCapacityWorker() {
    List<BlockWorkerInfo> workerInfos = generateBlockWorkerInfos(10, 0);
    BlockInfo blockInfo = new BlockInfo().setBlockId(1);
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockInfo(blockInfo)
        .setBlockWorkerInfos(workerInfos);
    assertFalse(POLICY.getWorker(options).isPresent());
  }

  private List<BlockWorkerInfo> generateBlockWorkerInfos(int numWorkers, int capacity) {
    ImmutableList.Builder<BlockWorkerInfo> workerInfoBuilder = ImmutableList.builder();
    for (int i = 0; i < numWorkers; i++) {
      // used bytes shouldn't matter in case of CapacityBaseHashPolicy; random number does not
      // affect the outcome of the policy
      long randomUsedBytes = ThreadLocalRandom.current().nextLong();
      WorkerNetAddress addr = new WorkerNetAddress().setHost(String.valueOf(i));
      BlockWorkerInfo workerInfo = new BlockWorkerInfo(addr, capacity, randomUsedBytes);
      workerInfoBuilder.add(workerInfo);
    }
    return workerInfoBuilder.build();
  }
}
