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

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;
import com.google.common.base.Preconditions;
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

public class RpcBenchPreparationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RpcBenchPreparationUtils.class);

  private static final String[] TIER_NAMES = new String[]{"MEM", "SSD", "HDD"};

  private static final long CAPACITY = 20L * 1024 * 1024 * 1024; // 20GB
  private static final Map<String, Long> CAPACITY_MEM = ImmutableMap.of("MEM", CAPACITY);
  private static final Map<String, Long> USED_MEM_EMPTY = ImmutableMap.of("MEM", 0L);
  private static final BlockStoreLocation BLOCK_LOCATION_MEM = new BlockStoreLocation("MEM", 0, "MEM");

  public static InstancedConfiguration sConf = InstancedConfiguration.defaults();

  private RpcBenchPreparationUtils() {}

  public static void prepareBlocksInMaster(Map<BlockStoreLocation, List<Long>> locToBlocks, ExecutorService pool, int concurrency) {
    // Calculate the ranges
    List<List<Long>> jobs = new ArrayList<>();
    for (Map.Entry<BlockStoreLocation, List<Long>> e : locToBlocks.entrySet()) {
      List<Long> v = e.getValue();

      jobs.addAll(Lists.partition(v, Math.min(v.size() / concurrency, 1_000)));
    }

    LOG.info("Split into {} jobs", jobs.size());
    for (List<Long> job : jobs) {
      LOG.info("Block ids: [{},{}]", job.get(0), job.get(job.size() - 1));
    }

    long blockSize = sConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);

    CompletableFuture[] futures = new CompletableFuture[jobs.size()];
    for (int i = 0; i < jobs.size(); i++) {
      List<Long> job = jobs.get(i);
      LOG.info("Submit job {}", i);
      CompletableFuture<Void> future = CompletableFuture.supplyAsync((Supplier<Void>) () -> {
              BlockMasterClient client =
                      new BlockMasterClient(MasterClientContext
                              .newBuilder(ClientContext.create(sConf))
                              .build());
              long finishedCount = 0;
              try {
                for (Long blockId : job) {
                  client.commitBlockInUfs(blockId, blockSize);
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

  public static Deque<Long> prepareWorkerIds(BlockMasterClient client, int numWorkers) throws IOException {
    // Prepare simulated workers
    Deque<Long> workerPool = new ArrayDeque<>();
    for (int i = 0; i < numWorkers; i++) {
      LOG.info("Preparing number {}", i);

      // prepare a worker ID
      int startPort = 9999;
      long workerId;
      String hostname = NetworkAddressUtils.getLocalHostName(500);
      LOG.info("Detected local hostname {}", hostname);
      WorkerNetAddress address = new WorkerNetAddress().setHost(hostname).setDataPort(startPort++).setRpcPort(startPort++);
      workerId = client.getId(address);
      LOG.info("Created worker ID {}", workerId);

      // Register worker
      List<String> tierAliases = new ArrayList<>();
      tierAliases.add("MEM");
      // TODO(jiacheng): propagate IOException better?
      client.register(workerId,
              tierAliases,
              CAPACITY_MEM,
              USED_MEM_EMPTY,
              ImmutableMap.of(BLOCK_LOCATION_MEM, new ArrayList<>()),
              ImmutableMap.of("MEM", new ArrayList<>()), // lost storage
              ImmutableList.of()); // extra config
      LOG.info("Worker {} registered", workerId);

      workerPool.offer(workerId);
    }
    Preconditions.checkState(workerPool.size() == numWorkers, "Expecting %s workers but registered %s",
            numWorkers, workerPool.size());
    LOG.info("All workers registered {}", workerPool);
    return workerPool;
  }

  public static Map<BlockStoreLocation, List<Long>> generateBlockIdOnTiers(String tiersConfig) {
    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();

    LOG.info("Tier and dir config is {}", tiersConfig);
    String[] tiers = tiersConfig.split(";");

    long blockIdStart = Long.MAX_VALUE;
    for (int i = 0; i < tiers.length; i++) {
      String tierConfig = tiers[i];
      LOG.info("Found tier {}", TIER_NAMES[i]);

      String[] dirConfigs = tierConfig.split(",");
      for (int j = 0; j < dirConfigs.length; j++) {
        String dir = dirConfigs[j];
        BlockStoreLocation loc = new BlockStoreLocation(TIER_NAMES[i], j);

        LOG.info("Found dir with {} blocks", dir);
        long num = Long.parseLong(dir);
        List<Long> blockIds = generateBlockIds(blockIdStart, num);

        blockMap.put(loc, blockIds);

        blockIdStart -= num;
      }
    }

    return blockMap;
  }

  public static List<Long> generateBlockIds(long start, long count) {
    LOG.info("Generating block Ids ({}, {}]", start - count, start);
    List<Long> list = new ArrayList<>();
    for (long i = 0; i < count; i++) {
      list.add(start - i);
    }
    return list;
  }
}
