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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.BlockChecksum;
import alluxio.grpc.GetBlockChecksumRequest;
import alluxio.grpc.GetBlockChecksumResponse;
import alluxio.grpc.GetStatusPOptions;
import alluxio.resource.CloseableResource;
import alluxio.util.CRC64;
import alluxio.util.CommonUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The utils functions for CRC64 check.
 */
public class CRC64CheckCommandUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLoadUtils.class);
  private static final int CHECKSUM_CALCULATION_BATCH_SIZE = Configuration.getInt(
      PropertyKey.USER_CLIENT_CHECKSUM_CALCULATION_BATCH_SIZE);
  private static final int SLOW_CHECKSUM_CALCULATION_REQUEST_THRESHOLD_MS = 120_000;

  /**
   * Gets the CRC64 checksum from UFS for a given path.
   *
   * @param fileSystem the file system
   * @param plainPath  the file path
   * @return the CRC64, if exists
   */
  public static long getUfsCRC64(
      FileSystem fileSystem, AlluxioURI plainPath)
      throws IOException, AlluxioException {
    URIStatus ufsStatus = fileSystem.getStatus(
        plainPath, GetStatusPOptions.newBuilder().setDirectUfsAccess(true).build());
    if (ufsStatus.getXAttr() == null || !ufsStatus.getXAttr().containsKey(Constants.CRC64_KEY)) {
      throw new UnsupportedOperationException("The ufs does not support CRC64 checksum");
    }
    return parseSignedOrUnsignedLong(new String(ufsStatus.getXAttr().get(Constants.CRC64_KEY)));
  }

  /**
   * Calculates the alluxio CRC64 value.
   *
   * @param fsContext  the file system context
   * @param fileSystem the file system
   * @param plainPath  the file path
   * @return the alluxio side CRC64, if the file is fully cached in alluxio and consistent
   */
  public static long calculateAlluxioCRC64(
      FileSystemContext fsContext, FileSystem fileSystem, AlluxioURI plainPath)
      throws IOException, AlluxioException {
    URIStatus status = fileSystem.getStatus(plainPath);
    if (status.isFolder()) {
      throw new IllegalStateException("The path is a folder");
    }
    if (status.getInAlluxioPercentage() != 100) {
      throw new IllegalStateException("The file is not fully cached in alluxio");
    }
    List<BlockLocationInfo> blockLocationInfoList = fileSystem.getBlockLocations(status);
    Map<WorkerNetAddress, List<Long>> blockIdsOnWorkers = new HashMap<>();
    for (BlockLocationInfo blockLocationInfo : blockLocationInfoList) {
      // One block can be persisted on multiple workers if passive replication is enabled,
      // even if multi replica is enabled.
      // If a block has multiple replications, we require all of them to have the same CRC64 value,
      // but tolerant with the possible missing of some replications on workers,
      // as long as at least 1 copy exists,
      for (WorkerNetAddress address : blockLocationInfo.getLocations()) {
        if (!blockIdsOnWorkers.containsKey(address)) {
          blockIdsOnWorkers.put(address, new ArrayList<>());
        }
        blockIdsOnWorkers.get(address)
            .add(blockLocationInfo.getBlockInfo().getBlockInfo().getBlockId());
      }
    }

    // RPC all workers that contain blocks of the file
    // If a worker contains too many blocks, the RPC will be sent by batches.
    // Details are in DefaultBlockWorkerClient
    Map<WorkerNetAddress, ListenableFuture<GetBlockChecksumResponse>> rpcFutures = new HashMap<>();
    for (Map.Entry<WorkerNetAddress, List<Long>> entry : blockIdsOnWorkers.entrySet()) {
      try (CloseableResource<BlockWorkerClient> blockWorkerClient
               = fsContext.acquireBlockWorkerClient(entry.getKey())) {
        GetBlockChecksumRequest request =
            GetBlockChecksumRequest.newBuilder().addAllBlockIds(entry.getValue()).build();
        rpcFutures.put(entry.getKey(), getBlockChecksumInBatches(blockWorkerClient.get(), request));
      }
    }

    // Collect the results;
    Map<Long, BlockChecksum> checksumMap = new HashMap<>();
    for (Map.Entry<WorkerNetAddress, ListenableFuture<GetBlockChecksumResponse>> entry :
        rpcFutures.entrySet()) {
      try {
        GetBlockChecksumResponse response = entry.getValue().get();
        for (Map.Entry<Long, BlockChecksum> checksumEntry : response.getChecksumMap().entrySet()) {
          long blockId = checksumEntry.getKey();
          BlockChecksum checksum = checksumEntry.getValue();
          if (checksumMap.containsKey(blockId)) {
            BlockChecksum checksumFromMap = checksumMap.get(blockId);
            if (checksumFromMap.getBlockLength() != checksum.getBlockLength()
                || !Objects.equals(checksumFromMap.getChecksum(), checksum.getChecksum())) {
              throw new RuntimeException("Block " + blockId
                  + " have multiple replicas across the workers but their checksum does not match"
              );
            }
          }
          checksumMap.put(blockId, checksum);
        }
      } catch (Exception e) {
        throw new RuntimeException("Rpc call failed for worker: " + entry.getKey(), e);
      }
    }

    long crc64Value = 0;
    // Calculate the crc64 checksum
    for (long blockId : status.getBlockIds()) {
      if (!checksumMap.containsKey(blockId)) {
        Optional<List<WorkerNetAddress>> addresses = blockLocationInfoList.stream().filter(
                it -> it.getBlockInfo().getBlockInfo().getBlockId() == blockId)
            .findFirst().map(BlockLocationInfo::getLocations);
        if (addresses.isPresent()) {
          throw new RuntimeException("Block " + blockId + " does not exist on any worker. "
              + "Master indicates the block is on the following workers: "
              + StringUtils.join(addresses.get(), ","));
        }
        throw new RuntimeException("Block " + blockId + " does not exist and cannot be found "
            + "from master block locations.");
      }
      BlockChecksum bcs = checksumMap.get(blockId);
      crc64Value = CRC64.combine(
              crc64Value, parseSignedOrUnsignedLong(bcs.getChecksum()), bcs.getBlockLength());
    }
    return crc64Value;
  }

  /**
   * Calculates the alluxio CRC for a given block.
   *
   * @param fsContext          the file system context
   * @param blockId            the block id to calculate
   * @param workerNetAddresses the worker addresses to calculate the blocks
   * @return a map of block checksum; If the block does not exist on the worker, it will
   * not show up in the map.
   */
  public static Map<WorkerNetAddress, BlockChecksum> calculateAlluxioCRCForBlock(
      FileSystemContext fsContext, long blockId, List<WorkerNetAddress> workerNetAddresses
  ) throws IOException {
    Map<WorkerNetAddress, BlockChecksum> results = new HashMap<>();
    Map<WorkerNetAddress, ListenableFuture<GetBlockChecksumResponse>> rpcFutures = new HashMap<>();
    for (WorkerNetAddress address : workerNetAddresses) {
      try (CloseableResource<BlockWorkerClient> blockWorkerClient
               = fsContext.acquireBlockWorkerClient(address)) {
        GetBlockChecksumRequest request =
            GetBlockChecksumRequest.newBuilder().addBlockIds(blockId).build();
        rpcFutures.put(address, getBlockChecksumInBatches(blockWorkerClient.get(), request));
      }
    }
    for (Map.Entry<WorkerNetAddress, ListenableFuture<GetBlockChecksumResponse>>
        entry : rpcFutures.entrySet()) {
      try {
        Map<Long, BlockChecksum> map = entry.getValue().get().getChecksumMap();
        if (!map.containsKey(blockId)) {
          LOG.warn("Block {} does not exist on worker {}", blockId, entry.getKey());
        } else {
          results.put(entry.getKey(), map.get(blockId));
        }
      } catch (Exception e) {
        LOG.error("Error calling RPC on worker {}", entry.getKey(), e);
      }
    }
    return results;
  }

  /**
   * A direct RPC call to the worker to calculate the CRC64 checksum.
   * @param client the worker client
   * @param request the request
   * @return the response
   */
  public static ListenableFuture<GetBlockChecksumResponse> getBlockChecksumRpc(
      BlockWorkerClient client, GetBlockChecksumRequest request
  ) throws IOException {
    return client.getBlockChecksum(request);
  }

  /**
   * Checks if the CRC64 checksum is consistent between alluxio and UFS.
   *
   * @param fsContext  the file system context
   * @param fileSystem the file system
   * @param plainPath  the file path
   * @return the crc64 value, if the check passes, otherwise an exception is thrown
   */
  public static long checkCRC64(
      FileSystemContext fsContext, FileSystem fileSystem, AlluxioURI plainPath)
      throws AlluxioException, IOException {
    long ufsCRC = getUfsCRC64(fileSystem, plainPath);
    long alluxioCRC = calculateAlluxioCRC64(fsContext, fileSystem, plainPath);
    if (ufsCRC != alluxioCRC) {
      throw new RuntimeException("CRC check of file " + plainPath + " failed. "
          + "ufs CRC: " + Long.toHexString(ufsCRC) + " alluxio CRC: "
          + Long.toHexString(alluxioCRC));
    }
    return ufsCRC;
  }

  private static ListenableFuture<GetBlockChecksumResponse> getBlockChecksumInBatches(
      BlockWorkerClient client, GetBlockChecksumRequest request) {
    // Non-batched mode
    if (request.getBlockIdsCount() < CHECKSUM_CALCULATION_BATCH_SIZE) {
      long startTs = CommonUtils.getCurrentMs();
      ListenableFuture<GetBlockChecksumResponse> future = client.getBlockChecksum(request);
      future.addListener(() -> {
        long timeElapsed = CommonUtils.getCurrentMs() - startTs;
        if (timeElapsed > SLOW_CHECKSUM_CALCULATION_REQUEST_THRESHOLD_MS) {
          LOG.warn(
              "Slow checksum calculation RPC for {} blocks, address {}, time elapsed {}ms ",
              request.getBlockIdsCount(), client.getAddress(), timeElapsed);
        }
      }, MoreExecutors.directExecutor());
      return future;
    }

    // Batched mode
    GetBlockChecksumResponse.Builder responseBuilder = GetBlockChecksumResponse.newBuilder();
    ListenableFuture<GetBlockChecksumResponse> chainedCalls =
        Futures.immediateFuture(GetBlockChecksumResponse.getDefaultInstance());
    List<List<Long>> blockIdsPerBatch =
        Lists.partition(request.getBlockIdsList(), CHECKSUM_CALCULATION_BATCH_SIZE);
    for (List<Long> blockIdsOfBatch : blockIdsPerBatch) {
      chainedCalls = Futures.transformAsync(chainedCalls, (previousResult) -> {
        responseBuilder.putAllChecksum(previousResult.getChecksumMap());
        GetBlockChecksumRequest requestOfBatch =
            GetBlockChecksumRequest.newBuilder().addAllBlockIds(blockIdsOfBatch).build();
        ListenableFuture<GetBlockChecksumResponse> future =
            client.getBlockChecksum(requestOfBatch);
        long startTs = CommonUtils.getCurrentMs();
        future.addListener(() -> {
          long timeElapsed = CommonUtils.getCurrentMs() - startTs;
          if (timeElapsed > SLOW_CHECKSUM_CALCULATION_REQUEST_THRESHOLD_MS) {
            LOG.warn(
                "Slow checksum calculation RPC for {} blocks, address {}, time elapsed {}ms ",
                blockIdsOfBatch.size(), client.getAddress(), timeElapsed);
          }
        }, MoreExecutors.directExecutor());
        return future;
      }, MoreExecutors.directExecutor());
    }
    return Futures.transform(chainedCalls, (lastResult) -> {
      responseBuilder.putAllChecksum(lastResult.getChecksumMap());
      return responseBuilder.build();
    }, MoreExecutors.directExecutor());
  }

  private static long parseSignedOrUnsignedLong(String input) {
    if (input.startsWith("-")) {
      return Long.parseLong(input);
    } else {
      return Long.parseUnsignedLong(input);
    }
  }
}
