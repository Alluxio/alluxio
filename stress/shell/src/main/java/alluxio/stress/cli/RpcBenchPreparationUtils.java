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

package alluxio.stress.cli;

import static alluxio.stress.rpc.TierAlias.MEM;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ConfigProperty;
import alluxio.master.MasterClientContext;
import alluxio.stress.rpc.TierAlias;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Utilities for the preparation step in RPC benchmark testing.
 */
public class RpcBenchPreparationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RpcBenchPreparationUtils.class);

  public static final long CAPACITY = 20L * 1024 * 1024 * 1024; // 20GB
  public static final Map<String, Long> CAPACITY_MEM = ImmutableMap.of(MEM.toString(), CAPACITY);
  public static final Map<String, Long> USED_MEM_EMPTY = ImmutableMap.of(MEM.toString(), 0L);
  public static final BlockStoreLocation BLOCK_LOCATION_MEM =
      new BlockStoreLocation(MEM.toString(), 0, MEM.toString());
  public static final Map<String, List<String>> LOST_STORAGE =
      ImmutableMap.of(MEM.toString(), ImmutableList.of());
  public static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();

  private static InstancedConfiguration sConf = InstancedConfiguration.defaults();

  private RpcBenchPreparationUtils() {}

  /**
   * Prepare all relevant block IDs on the master side concurrently.
   *
   * @param locToBlocks a map from block location to block IDs
   * @param pool the thread pool to submit to
   * @param concurrency number of threads/concurrent clients
   */
  public static void prepareBlocksInMaster(Map<BlockStoreLocation, List<Long>> locToBlocks,
                                           ExecutorService pool,
                                           int concurrency) {
    // Partition the wanted block IDs to smaller jobs in order to utilize concurrency
    List<List<Long>> jobs = new ArrayList<>();
    for (Map.Entry<BlockStoreLocation, List<Long>> e : locToBlocks.entrySet()) {
      List<Long> v = e.getValue();
      jobs.addAll(Lists.partition(v, Math.min(v.size() / concurrency, 1_000)));
    }

    LOG.info("Split block ID generation into {} jobs", jobs.size());
    for (List<Long> job : jobs) {
      LOG.info("Block ids: [{},{}]", job.get(0), job.get(job.size() - 1));
    }

    long blockSize = sConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    CompletableFuture[] futures = new CompletableFuture[jobs.size()];
    for (int i = 0; i < jobs.size(); i++) {
      List<Long> job = jobs.get(i);
      LOG.info("Generating block IDs in range {}", i);
      CompletableFuture<Void> future = CompletableFuture.supplyAsync((Supplier<Void>) () -> {
        BlockMasterClient client =
            new BlockMasterClient(MasterClientContext
                .newBuilder(ClientContext.create(sConf))
                .build());
        long finishedCount = 0;
        try {
          for (Long blockId : job) {
            client.commitBlockInUfs(blockId, blockSize);
            finishedCount++;
          }
        } catch (IOException e) {
          LOG.error("Failed to commitBlockInUfs with finishedCount {}", finishedCount, e);
        }
        return null;
      }, pool);
      futures[i] = (future);
    }

    LOG.info("Collect all results");
    CompletableFuture.allOf(futures).join();
  }

  /**
   * Reserves a list of worker IDs from the master.
   */
  static Deque<Long> prepareWorkerIds(BlockMasterClient client, int numWorkers) throws IOException {
    Deque<Long> workerPool = new ArrayDeque<>();
    int freePort = 40000;
    for (int i = 0; i < numWorkers; i++) {
      LOG.info("Preparing worker {}", i);

      // Prepare a worker ID
      // The addresses are different in order to get different worker IDs
      long workerId;
      String hostname = NetworkAddressUtils.getLocalHostName(500);
      LOG.info("Detected local hostname {}", hostname);
      WorkerNetAddress address = new WorkerNetAddress().setHost(hostname)
              .setDataPort(freePort++)
              .setRpcPort(freePort++)
              .setWebPort(freePort++);
      workerId = client.getId(address);
      LOG.info("Created worker ID {} on {}", workerId, address);
      workerPool.offer(workerId);
    }
    return workerPool;
  }

  static void registerWorkers(BlockMasterClient client, Deque<Long> workerIds) throws IOException {
    for (long w : workerIds) {
      LOG.info("Worker {} registering", w);
      client.register(w,
          ImmutableList.of(MEM.toString()),
          CAPACITY_MEM,
          USED_MEM_EMPTY,
          ImmutableMap.of(BLOCK_LOCATION_MEM, ImmutableList.of()),
          LOST_STORAGE, // lost storage
          EMPTY_CONFIG); // extra config
    }
    LOG.info("All workers registered");
  }

  /**
   * Generates block IDs according to the storage tier/dir setup.
   * In order to avoid block ID colliding with existing blocks, this will generate IDs
   * decreasingly from the {@link Long#MAX_VALUE}.
   */
  static Map<BlockStoreLocation, List<Long>> generateBlockIdOnTiers(
      Map<TierAlias, List<Integer>> tiersConfig) {
    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();

    long blockIdStart = Long.MAX_VALUE;
    for (Map.Entry<TierAlias, List<Integer>> tierConfig : tiersConfig.entrySet()) {
      List<Integer> dirConfigs = tierConfig.getValue();
      for (int i = 0; i < dirConfigs.size(); i++) {
        int dirNumBlocks = dirConfigs.get(i);
        LOG.info("Found dir on tier {} with {} blocks", tierConfig.getKey(), dirNumBlocks);
        BlockStoreLocation loc = new BlockStoreLocation(tierConfig.getKey().toString(), i);
        List<Long> blockIds = generateDecreasingNumbers(blockIdStart, dirNumBlocks);
        blockMap.put(loc, blockIds);
        blockIdStart -= dirNumBlocks;
      }
    }
    return blockMap;
  }

  private static List<Long> generateDecreasingNumbers(long start, int count) {
    LOG.info("Generating block Ids ({}, {}]", start - count, start);
    List<Long> list = new ArrayList<>(count);
    for (long i = 0; i < count; i++) {
      list.add(start - i);
    }
    return list;
  }
}
