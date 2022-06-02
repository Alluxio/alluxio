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

package alluxio.master.block;

import alluxio.RpcUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.exception.RegisterLeaseNotFoundException;
import alluxio.grpc.BlockHeartbeatPRequest;
import alluxio.grpc.BlockHeartbeatPResponse;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.CommitBlockInUfsPRequest;
import alluxio.grpc.CommitBlockInUfsPResponse;
import alluxio.grpc.CommitBlockPRequest;
import alluxio.grpc.CommitBlockPResponse;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.grpc.GetRegisterLeasePResponse;
import alluxio.grpc.GetWorkerIdPRequest;
import alluxio.grpc.GetWorkerIdPResponse;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.metrics.Metric;
import alluxio.proto.meta.Block;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is a gRPC handler for block master RPCs invoked by an Alluxio worker.
 */
public final class BlockMasterWorkerServiceHandler extends
    BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterWorkerServiceHandler.class);

  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterWorkerServiceHandler}.
   *
   * @param blockMaster the {@link BlockMaster} the handler uses internally
   */
  public BlockMasterWorkerServiceHandler(BlockMaster blockMaster) {
    Preconditions.checkNotNull(blockMaster, "blockMaster");
    mBlockMaster = blockMaster;
  }

  @Override
  public void blockHeartbeat(BlockHeartbeatPRequest request,
      StreamObserver<BlockHeartbeatPResponse> responseObserver) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Block heartbeat request is {} bytes, {} added blocks and {} removed blocks",
              request.getSerializedSize(),
              request.getAddedBlocksCount(),
              request.getRemovedBlockIdsCount());
    }

    final long workerId = request.getWorkerId();
    final Map<String, Long> capacityBytesOnTiers =
        request.getOptions().getCapacityBytesOnTiersMap();
    final Map<String, Long> usedBytesOnTiers = request.getUsedBytesOnTiersMap();
    final List<Long> removedBlockIds = request.getRemovedBlockIdsList();
    final Map<String, StorageList> lostStorageMap = request.getLostStorageMap();

    final Map<Block.BlockLocation, List<Long>> addedBlocksMap =
        reconstructBlocksOnLocationMap(request.getAddedBlocksList(), workerId);

    final List<Metric> metrics = request.getOptions().getMetricsList()
        .stream().map(Metric::fromProto).collect(Collectors.toList());

    RpcUtils.call(LOG, () ->
        BlockHeartbeatPResponse.newBuilder().setCommand(mBlockMaster.workerHeartbeat(workerId,
          capacityBytesOnTiers, usedBytesOnTiers, removedBlockIds, addedBlocksMap,
            lostStorageMap, metrics)).build(),
        "blockHeartbeat", "request=%s", responseObserver, request);
  }

  @Override
  public void commitBlock(CommitBlockPRequest request,
      StreamObserver<CommitBlockPResponse> responseObserver) {

    final long workerId = request.getWorkerId();
    final long usedBytesOnTier = request.getUsedBytesOnTier();
    final String tierAlias = request.getTierAlias();
    final long blockId = request.getBlockId();
    final String mediumType = request.getMediumType();
    final long length = request.getLength();

    RpcUtils.call(LOG, () -> {
      mBlockMaster.commitBlock(workerId, usedBytesOnTier, tierAlias,
          mediumType, blockId, length);
      return CommitBlockPResponse.getDefaultInstance();
    }, "commitBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void commitBlockInUfs(CommitBlockInUfsPRequest request,
      StreamObserver<CommitBlockInUfsPResponse> responseObserver) {

    RpcUtils.call(LOG,
            () -> {
              mBlockMaster.commitBlockInUFS(request.getBlockId(), request.getLength());
              return CommitBlockInUfsPResponse.getDefaultInstance();
            }, "commitBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void getWorkerId(GetWorkerIdPRequest request,
      StreamObserver<GetWorkerIdPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetWorkerIdPResponse.newBuilder()
        .setWorkerId(mBlockMaster.getWorkerId(GrpcUtils.fromProto(request.getWorkerNetAddress())))
        .build(), "getWorkerId", "request=%s", responseObserver, request);
  }

  @Override
  public void requestRegisterLease(GetRegisterLeasePRequest request,
                                   StreamObserver<GetRegisterLeasePResponse> responseObserver) {
    RpcUtils.call(LOG, () ->
        GrpcUtils.toProto(request.getWorkerId(), mBlockMaster.tryAcquireRegisterLease(request)),
        "getRegisterLease", "request=%s", responseObserver, request);
  }

  @Override
  public void registerWorker(RegisterWorkerPRequest request,
      StreamObserver<RegisterWorkerPResponse> responseObserver) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Register worker request is {} bytes, containing {} blocks",
              request.getSerializedSize(),
              request.getCurrentBlocksCount());
    }

    final long workerId = request.getWorkerId();
    RegisterWorkerPOptions options = request.getOptions();
    RpcUtils.call(LOG,
        () -> {
          // The exception will be propagated to the worker side and the worker should retry.
          if (Configuration.getBoolean(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED)
              && !mBlockMaster.hasRegisterLease(workerId)) {
            String errorMsg = String.format("Worker %s does not have a lease or the lease "
                + "has expired. The worker should acquire a new lease and retry to register.",
                workerId);
            LOG.warn(errorMsg);
            throw new RegisterLeaseNotFoundException(errorMsg);
          }
          LOG.debug("Worker {} proceeding to register...", workerId);
          final List<String> storageTiers = request.getStorageTiersList();
          final Map<String, Long> totalBytesOnTiers = request.getTotalBytesOnTiersMap();
          final Map<String, Long> usedBytesOnTiers = request.getUsedBytesOnTiersMap();
          final Map<String, StorageList> lostStorageMap = request.getLostStorageMap();

          final Map<Block.BlockLocation, List<Long>> currBlocksOnLocationMap =
                  reconstructBlocksOnLocationMap(request.getCurrentBlocksList(), workerId);

          // If the register is unsuccessful, the lease will be kept around until the expiry.
          // The worker can retry and use the existing lease.
          mBlockMaster.workerRegister(workerId, storageTiers, totalBytesOnTiers, usedBytesOnTiers,
                  currBlocksOnLocationMap, lostStorageMap, options);
          LOG.info("Worker {} finished registering, releasing its lease.", workerId);
          mBlockMaster.releaseRegisterLease(workerId);
          return RegisterWorkerPResponse.getDefaultInstance();
        }, "registerWorker", true, "request=%s", responseObserver, workerId);
  }

  @Override
  public io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPRequest> registerWorkerStream(
      io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> responseObserver) {
    return new RegisterStreamObserver(mBlockMaster, responseObserver);
  }

  /**
   * This converts the flattened list of block locations back to a map.
   * This relies on the unique guarantee from the worker-side serialization.
   * If a duplicated key is seen, an AssertionError will be thrown.
   * The key is {@link Block.BlockLocation}, where the hash code is determined by
   * tier alias and medium type.
   * */
  static Map<Block.BlockLocation, List<Long>> reconstructBlocksOnLocationMap(
          List<LocationBlockIdListEntry> entries, long workerId) {
    return entries.stream().collect(
        Collectors.toMap(
            e -> Block.BlockLocation.newBuilder().setTier(e.getKey().getTierAlias())
                .setMediumType(e.getKey().getMediumType()).setWorkerId(workerId).build(),
            e -> e.getValue().getBlockIdList(),
            /*
             * The merger function is invoked on key collisions to merge the values.
             * In fact this merger should never be invoked because the list is deduplicated
             * by {@link BlockMasterClient} before sending to the master.
             * Therefore we just fail on merging.
             */
            (e1, e2) -> {
              String entryReport = entries.stream().map((e) -> e.getKey().toString())
                      .collect(Collectors.joining(","));
              throw new AssertionError(
                String.format("Duplicate locations found for worker %s "
                    + "with LocationBlockIdListEntry objects %s", workerId, entryReport));
            }));
  }
}
