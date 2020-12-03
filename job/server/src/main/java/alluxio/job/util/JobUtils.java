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
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.LocalFirstPolicy;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class to make it easier to write jobs.
 */
public final class JobUtils {
  private static final String LOCAL_HOST_NAME =
      NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC,
      ServerConfiguration.global());

  private static final IndexDefinition<BlockWorkerInfo, WorkerNetAddress> WORKER_ADDRESS_INDEX =
      new IndexDefinition<BlockWorkerInfo, WorkerNetAddress>(true) {
        @Override
        public WorkerNetAddress getFieldValue(BlockWorkerInfo o) {
          return o.getNetAddress();
        }
      };

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

  /**
   * Loads a block into the local worker. If the block doesn't exist in Alluxio, it will be read
   * from the UFS.
   *
   * @param status the file status
   * @param context filesystem context
   * @param blockId the id of the block to load
   */
  public static void loadBlock(URIStatus status, FileSystemContext context, long blockId)
      throws IOException {
    AlluxioBlockStore blockStore = AlluxioBlockStore.create(context);
    List<BlockWorkerInfo> workerInfoList = context.getCachedWorkers();
    WorkerNetAddress localNetAddress = null;

    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(LOCAL_HOST_NAME)) {
        localNetAddress = workerInfo.getNetAddress();
        break;
      }
    }
    if (localNetAddress == null) {
      throw new NotFoundException(ExceptionMessage.NO_LOCAL_BLOCK_WORKER_REPLICATE_TASK
          .getMessage(blockId));
    }

    OpenFilePOptions openOptions =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();

    AlluxioConfiguration conf = ServerConfiguration.global();
    InStreamOptions inOptions = new InStreamOptions(status, openOptions, conf);
    // Set read location policy always to local first for loading blocks for job tasks
    inOptions.setUfsReadLocationPolicy(BlockLocationPolicy.Factory.create(
        LocalFirstPolicy.class.getCanonicalName(), conf));

    // use -1 to reuse the existing block size for this block
    byte[] buf = new byte[8 * Constants.MB];
    try (InputStream inputStream = blockStore.getInStream(blockId, inOptions)) {
      while (inputStream.read(buf) != -1) {
      }
    }
  }

  private JobUtils() {} // Utils class not intended for instantiation.
}
