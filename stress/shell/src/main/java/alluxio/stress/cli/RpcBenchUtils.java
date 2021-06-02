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
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RpcBenchUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RpcBenchUtils.class);

  public static final long BLOCK_ID_START = 100_000_000L;
  public static final int CONCURRENCY = 20;

  public static InstancedConfiguration sConf = InstancedConfiguration.defaults();

  private RpcBenchUtils() {}

  public static void prepareBlocksInMaster(Map<BlockStoreLocation, List<Long>> locToBlocks) throws InterruptedException {
    // Calculate the ranges
    List<List<Long>> jobs = new ArrayList<>();
    for (Map.Entry<BlockStoreLocation, List<Long>> e : locToBlocks.entrySet()) {
      List<Long> v = e.getValue();

      jobs.addAll(Lists.partition(v, Math.min(v.size() / CONCURRENCY, 1_000)));
    }

    // Report
    LOG.info("Prepared {} jobs", jobs.size());
    for (List<Long> job : jobs) {
      LOG.info("Block ids: [{},{}]", job.get(0), job.get(job.size() - 1));
    }
    CountDownLatch jobsFinished = new CountDownLatch(jobs.size());

    // Create a threadpool to prepare blocks
    ExecutorService pool = null;
    try {
      pool = ExecutorServiceFactories.fixedThreadPool("prepare-blocks-worker", CONCURRENCY)
              .create();
      long blockSize = sConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);

      for (List<Long> job : jobs) {
        pool.submit(() -> {
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
          } finally {
            jobsFinished.countDown();
          }
        });
      }

      return;
    } catch (Exception e) {
      LOG.error("Failed to execute RPC in pool", e);
      return;
    } finally {

      // Wait for all jobs to finish then destroy
      System.out.println("Waiting for jobs to finish...");
      jobsFinished.await();
      System.out.println("Jobs finished, quitting...");
      if (pool != null) {
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  public static Map<BlockStoreLocation, List<Long>> generateBlockIdOnTiers(String tiersConfig) {
    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();

    LOG.info("Tier and dir config is {}", tiersConfig);
    String[] tierNames = new String[]{"MEM", "SSD", "HDD"};
    String[] tiers = tiersConfig.split(";");

    long blockIdStart = BLOCK_ID_START;
    for (int i = 0; i < tiers.length; i++) {
      String tierConfig = tiers[i];
      LOG.info("Found tier {}", tierNames[i]);

      String[] dirConfigs = tierConfig.split(",");
      for (int j = 0; j < dirConfigs.length; j++) {
        String dir = dirConfigs[j];
        BlockStoreLocation loc = new BlockStoreLocation(tierNames[i], j);

        LOG.info("Found dir with {} blocks", dir);
        long num = Long.parseLong(dir);
        List<Long> blockIds = generateBlockIds(blockIdStart, num);

        blockMap.put(loc, blockIds);

        blockIdStart += num;
      }
    }

    return blockMap;
  }

  public static List<Long> generateBlockIds(long start, long count) {
    LOG.info("Generating block Ids [{}, {})", start, start + count);
    List<Long> list = new ArrayList<>();
    for (long i = 0; i < count; i++) {
      list.add(start + i);
    }
    return list;
  }



  public static void main(String[] args) throws Exception {
    System.out.format("Arguments: %s%n", Arrays.toString(args));
    String tierConfig = args[0];

    Map<BlockStoreLocation, List<Long>> locToBlocks = generateBlockIdOnTiers(tierConfig);

    prepareBlocksInMaster(locToBlocks);
  }
}
