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

package alluxio.job.util;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class to make it easier to write jobs.
 */
public final class JobUtils {
  // a read buffer that should be ignored
  private static final byte[] READ_BUF = new byte[8 * Constants.MB];
  private static final IndexDefinition<BlockWorkerInfo, WorkerNetAddress> WORKER_ADDRESS_INDEX =
      IndexDefinition.ofUnique(BlockWorkerInfo::getNetAddress);

  /**
   * Returns whichever specified worker stores the most blocks from the block info list.
   *
   * @param workers a list of workers to consider
   * @param fileBlockInfos a list of file block information
   * @return a worker address storing the most blocks from the list
   */
  public static BlockWorkerInfo getWorkerWithMostBlocks(List<BlockWorkerInfo> workers,
      List<FileBlockInfo> fileBlockInfos) {
    // Index workers by their addresses.
    IndexedSet<BlockWorkerInfo> addressIndexedWorkers = new IndexedSet<>(WORKER_ADDRESS_INDEX);
    addressIndexedWorkers.addAll(workers);

    // Use ConcurrentMap for putIfAbsent. A regular Map works in Java 8.
    ConcurrentMap<BlockWorkerInfo, Integer> blocksPerWorker = Maps.newConcurrentMap();
    int maxBlocks = 0;
    BlockWorkerInfo mostBlocksWorker = null;
    for (FileBlockInfo fileBlockInfo : fileBlockInfos) {
      for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
        BlockWorkerInfo worker = addressIndexedWorkers.getFirstByField(WORKER_ADDRESS_INDEX,
            location.getWorkerAddress());
        if (worker == null) {
          // We can only choose workers in the workers list.
          continue;
        }
        blocksPerWorker.putIfAbsent(worker, 0);
        int newBlockCount = blocksPerWorker.get(worker) + 1;
        blocksPerWorker.put(worker, newBlockCount);
        if (newBlockCount > maxBlocks) {
          maxBlocks = newBlockCount;
          mostBlocksWorker = worker;
        }
      }
    }
    return mostBlocksWorker;
  }

  private JobUtils() {} // Utils class not intended for instantiation.
}
