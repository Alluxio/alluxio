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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.BlockHeartbeatPRequest;
import alluxio.grpc.BlockHeartbeatPResponse;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.CommitBlockInUfsPRequest;
import alluxio.grpc.CommitBlockInUfsPResponse;
import alluxio.grpc.CommitBlockPRequest;
import alluxio.grpc.CommitBlockPResponse;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GetWorkerIdPRequest;
import alluxio.grpc.GetWorkerIdPResponse;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.RegisterWorkerStreamPOptions;
import alluxio.grpc.RegisterWorkerStreamPResponse;
import alluxio.grpc.StorageList;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.metrics.Metric;
import alluxio.proto.meta.Block;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
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

    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<BlockHeartbeatPResponse>) () ->
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

    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<CommitBlockPResponse>) () -> {
      mBlockMaster.commitBlock(workerId, usedBytesOnTier, tierAlias,
          mediumType, blockId, length);
      return CommitBlockPResponse.getDefaultInstance();
    }, "commitBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void commitBlockInUfs(CommitBlockInUfsPRequest request,
      StreamObserver<CommitBlockInUfsPResponse> responseObserver) {

    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<CommitBlockInUfsPResponse>) () -> {
          mBlockMaster.commitBlockInUFS(request.getBlockId(), request.getLength());
          return CommitBlockInUfsPResponse.getDefaultInstance();
        }, "commitBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void getWorkerId(GetWorkerIdPRequest request,
      StreamObserver<GetWorkerIdPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetWorkerIdPResponse>) () -> {
      return GetWorkerIdPResponse.newBuilder()
          .setWorkerId(mBlockMaster.getWorkerId(GrpcUtils.fromProto(request.getWorkerNetAddress())))
          .build();
    }, "getWorkerId", "request=%s", responseObserver, request);
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
    final List<String> storageTiers = request.getStorageTiersList();
    final Map<String, Long> totalBytesOnTiers = request.getTotalBytesOnTiersMap();
    final Map<String, Long> usedBytesOnTiers = request.getUsedBytesOnTiersMap();
    final Map<String, StorageList> lostStorageMap = request.getLostStorageMap();

    final Map<Block.BlockLocation, List<Long>> currBlocksOnLocationMap =
        reconstructBlocksOnLocationMap(request.getCurrentBlocksList(), workerId);

    RegisterWorkerPOptions options = request.getOptions();
    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<RegisterWorkerPResponse>) () -> {
          mBlockMaster.workerRegister(workerId, storageTiers, totalBytesOnTiers, usedBytesOnTiers,
              currBlocksOnLocationMap, lostStorageMap, options);
          return RegisterWorkerPResponse.getDefaultInstance();
        }, "registerWorker", "request=%s", responseObserver, request);
  }

  @Override
  public io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerStreamPRequest> registerWorkerStream(
          io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerStreamPResponse> responseObserver) {
    // TODO(jiacheng): Move to a separate class
    return new StreamObserver<alluxio.grpc.RegisterWorkerStreamPRequest>() {
      private WorkerRegisterContext mContext;

      @Override
      public void onNext(alluxio.grpc.RegisterWorkerStreamPRequest chunk) {
        final long workerId = chunk.getWorkerId();
        final boolean isHead = chunk.getIsHead();
        LOG.info("{} - Register worker request is {} bytes, containing {} LocationBlockIdListEntry. Worker {}, isHead {}",
                Thread.currentThread().getId(),
                chunk.getSerializedSize(),
                chunk.getCurrentBlocksCount(),
                workerId,
                isHead);

        // TODO(jiacheng): potential risk from 'this' ref escape?
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerStreamPRequest> requestObserver = this;
        String methodName = isHead ? "registerWorkerStart" : "registerWorkerStream";

        RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<RegisterWorkerStreamPResponse>() {
          @Override
          public RegisterWorkerStreamPResponse call() throws Exception {
            // Initialize the context on the 1st message
            synchronized (requestObserver) {
              if (mContext == null) {
                LOG.debug("Initializing the WorkerRegisterContext on the 1st request");
                Preconditions.checkState(isHead, "WorkerRegisterContext is not initialized but the request is not the 1st in a stream");

                LOG.debug("Initializing context for {}", workerId);
                mContext = WorkerRegisterContext.create(mBlockMaster, workerId, requestObserver, responseObserver);
                LOG.debug("Context created for {}", workerId);
              }
            }

            Preconditions.checkState(mContext != null, "Stream message received from the client side but the context is not initialized");
            Preconditions.checkState(mContext.isOpen(), "Context is not open");
            Preconditions.checkState(mContext.mWorker.checkLocks(EnumSet.of(WorkerMetaLockSection.STATUS, WorkerMetaLockSection.USAGE, WorkerMetaLockSection.BLOCKS), false));

            if (isHead) {
              mBlockMaster.workerRegisterStart(mContext, chunk);
            } else {
              mBlockMaster.workerRegisterStream(mContext, chunk);
            }
            mContext.updateTs();
            // Return an ACK to the worker so it sends the next batch
            return RegisterWorkerStreamPResponse.newBuilder().build();
          }

          @Override
          public void exceptionCaught(Throwable e) {
            // onError will close the resources
            responseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
            Preconditions.checkState(!mContext.isOpen(),
                "The WorkerRegisterContext is not properly closed after error %s "
                    + "aborted the register stream!", e.getMessage());
          }
        }, methodName, true, false, responseObserver, "Worker=%s", workerId);
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("Error receiving the streaming register call", t);
        try {
          closeContext();
        } catch (IOException e) {
          LOG.error("Failed to close the WorkerRegisterContext for {}: ", mContext.mWorkerId, e);
        }

        // TODO(jiacheng): unit test on what exceptions are thrown
        responseObserver.onError(AlluxioStatusException.fromThrowable(t).toGrpcStatusException());
      }

      void closeContext() throws IOException {
        synchronized (this) {
          if (mContext!= null) {
            LOG.debug("Unlocking worker {}", mContext.mWorkerId);
            mContext.close();
            LOG.debug("Context closed");
          }
        }
      }

      @Override
      public void onCompleted() {
        LOG.info("{} - Register stream completed on the client side", Thread.currentThread().getId());

        String methodName = "registerWorkerComplete";
        Preconditions.checkState(mContext != null, "Complete message received from the client side but the context is not initialized");

        Preconditions.checkState(mContext.mWorker.checkLocks(EnumSet.of(WorkerMetaLockSection.STATUS, WorkerMetaLockSection.USAGE, WorkerMetaLockSection.BLOCKS), false));

        RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<RegisterWorkerStreamPResponse>() {
            @Override
            public RegisterWorkerStreamPResponse call() throws Exception {
              Preconditions.checkState(mContext != null, "Complete message received from the client side but the context is not initialized");
              Preconditions.checkState(mContext.isOpen(), "Context is not open");

              mContext.updateTs();
              mBlockMaster.workerRegisterFinish(mContext);

              // Reclaim resources
              try {
                closeContext();
              } catch (IOException e) {
                e.printStackTrace();
              }

              // No response because sendResponse=false
              return null;
            }

            @Override
            public void exceptionCaught(Throwable e) {
              // onError will close the resources
              responseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
              Preconditions.checkState(!mContext.isOpen(),
                      "The WorkerRegisterContext is not properly closed after error %s "
                              + "happened on completing the register stream!", e.getMessage());
            }
          }, methodName, false, true, responseObserver, "WorkerId=%s", mContext.mWorkerId);
      }
    };
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
            /**
             * The merger function is invoked on key collisions to merge the values.
             * In fact this merger should never be invoked because the list is deduplicated
             * by {@link BlockMasterClient#heartbeat} before sending to the master.
             * Therefore we just fail on merging.
             */
            (e1, e2) -> {
              throw new AssertionError(
                String.format("Request contains two block id lists for the "
                  + "same BlockLocation.%nExisting: %s%n New: %s", e1, e2));
            }));
  }
}
