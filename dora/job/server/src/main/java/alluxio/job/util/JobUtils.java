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
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.LocalFirstPolicy;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.InputStream;
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

  private static void loadThroughCacheRequest(URIStatus status, FileSystemContext context,
      long blockId, AlluxioConfiguration conf, WorkerNetAddress localNetAddress)
      throws IOException {
    BlockStoreClient blockStore = BlockStoreClient.create(context);
    OpenFilePOptions openOptions =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build();
    InStreamOptions inOptions = new InStreamOptions(status, openOptions, conf, context);
    BlockLocationPolicy policy =
        BlockLocationPolicy.Factory.create(LocalFirstPolicy.class, conf);
    inOptions.setUfsReadLocationPolicy(policy);
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = inOptions.getOpenUfsBlockOptions(blockId);
    BlockInfo info = Preconditions.checkNotNull(status.getBlockInfo(blockId));
    long blockLength = info.getLength();
    Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource> dataSourceAndType = blockStore
        .getDataSourceAndType(status.getBlockInfo(blockId), status, policy, ImmutableMap.of());
    WorkerNetAddress dataSource = dataSourceAndType.getFirst();
    String host = dataSource.getHost();
    // issues#11172: If the worker is in a container, use the container hostname
    // to establish the connection.
    if (!dataSource.getContainerHost().equals("")) {
      host = dataSource.getContainerHost();
    }
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(openUfsBlockOptions).setSourceHost(host)
        .setSourcePort(dataSource.getDataPort()).build();
    try (CloseableResource<BlockWorkerClient> blockWorker =
        context.acquireBlockWorkerClient(localNetAddress)) {
      blockWorker.get().cache(request);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static void loadThroughRead(URIStatus status, FileSystemContext context, long blockId,
      AlluxioConfiguration conf) throws IOException {
    BlockStoreClient blockStore = BlockStoreClient.create(context);
    OpenFilePOptions openOptions =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build();
    InStreamOptions inOptions = new InStreamOptions(status, openOptions, conf, context);
    inOptions.setUfsReadLocationPolicy(BlockLocationPolicy.Factory.create(
        LocalFirstPolicy.class, conf));
    BlockInfo info = Preconditions.checkNotNull(status.getBlockInfo(blockId));
    try (InputStream inputStream = blockStore.getInStream(info, inOptions, ImmutableMap.of())) {
      while (inputStream.read(READ_BUF) != -1) {}
    }
  }

  private JobUtils() {} // Utils class not intended for instantiation.
}
