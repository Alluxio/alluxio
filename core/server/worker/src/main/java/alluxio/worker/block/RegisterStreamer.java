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

package alluxio.worker.block;

import alluxio.ProjectConstants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This class oversees the logic of registering with the master using a stream of
 * {@link RegisterWorkerPRequest}.
 * The stream lifecycle management lives internal to this instance.
 */
public class RegisterStreamer implements Iterator<RegisterWorkerPRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStreamer.class);

  private static final int MAX_BATCHES_IN_FLIGHT = 2;

  private final long mWorkerId;
  private final List<String> mStorageTierAliases;
  private final Map<String, Long> mTotalBytesOnTiers;
  private final Map<String, Long> mUsedBytesOnTiers;
  private final RegisterWorkerPOptions mOptions;
  private final Map<String, StorageList> mLostStorageMap;

  private int mBatchNumber;
  private final BlockMapIterator mBlockMapIterator;

  // For internal flow control and state mgmt
  private final CountDownLatch mAckLatch;
  private final CountDownLatch mFinishLatch;
  private final Semaphore mBucket = new Semaphore(MAX_BATCHES_IN_FLIGHT);
  private final AtomicReference<Throwable> mError = new AtomicReference<>();

  private final int mResponseTimeoutMs;
  private final int mDeadlineMs;
  private final int mCompleteTimeoutMs;

  private final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub mAsyncClient;
  private final StreamObserver<RegisterWorkerPResponse> mMasterResponseObserver;
  // This cannot be final because it is created on registerWithMaster()
  private StreamObserver<RegisterWorkerPRequest> mWorkerRequestObserver;

  /**
   * Constructor.
   *
   * @param asyncClient the grpc client
   * @param workerId the worker ID
   * @param storageTierAliases storage/tier setup from the configuration
   * @param totalBytesOnTiers the capacity of each tier
   * @param usedBytesOnTiers the current usage of each tier
   * @param currentBlocksOnLocation the blocks in each tier/dir
   * @param lostStorage the lost storage paths
   * @param configList the configuration properties
   */
  @VisibleForTesting
  public RegisterStreamer(
      final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
      final long workerId, final List<String> storageTierAliases,
      final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
      final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
      final Map<String, List<String>> lostStorage,
      final List<ConfigProperty> configList) {
    this(asyncClient, workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
        lostStorage, configList, new BlockMapIterator(currentBlocksOnLocation));
  }

  /**
   * Constructor.
   *
   * @param asyncClient the grpc client
   * @param workerId the worker ID
   * @param storageTierAliases storage/tier setup from the configuration
   * @param totalBytesOnTiers the capacity of each tier
   * @param usedBytesOnTiers the current usage of each tier
   * @param lostStorage the lost storage paths
   * @param configList the configuration properties
   * @param blockListIterator an iterator used to iterate the blocks
   */
  public RegisterStreamer(
      final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
      final long workerId, final List<String> storageTierAliases,
      final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
      final Map<String, List<String>> lostStorage,
      final List<ConfigProperty> configList,
      BlockMapIterator blockListIterator) {
    mAsyncClient = asyncClient;
    mWorkerId = workerId;
    mStorageTierAliases = storageTierAliases;
    mTotalBytesOnTiers = totalBytesOnTiers;
    mUsedBytesOnTiers = usedBytesOnTiers;

    final BuildVersion buildVersion = BuildVersion.newBuilder()
        .setVersion(ProjectConstants.VERSION)
        .setRevision(ProjectConstants.REVISION)
        .build();
    mOptions = RegisterWorkerPOptions.newBuilder().addAllConfigs(configList)
        .setBuildVersion(buildVersion).build();
    mLostStorageMap = lostStorage.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));
    mBatchNumber = 0;
    mBlockMapIterator = blockListIterator;

    mAckLatch = new CountDownLatch(mBlockMapIterator.getBatchCount());
    mFinishLatch = new CountDownLatch(1);

    mResponseTimeoutMs =
        (int) Configuration.getMs(PropertyKey.WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT);
    mDeadlineMs =
        (int) Configuration.getMs(PropertyKey.WORKER_REGISTER_STREAM_DEADLINE);
    mCompleteTimeoutMs =
        (int) Configuration.getMs(PropertyKey.WORKER_REGISTER_STREAM_COMPLETE_TIMEOUT);

    mMasterResponseObserver = new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse res) {
        LOG.debug("Worker {} - Received ACK {}", mWorkerId, res);
        mBucket.release();
        mAckLatch.countDown();
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("Worker {} - received error from server, marking the stream as closed: ",
            mWorkerId, t);
        mError.set(t);
        mFinishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        LOG.info("{} - Complete message received from the server. Closing stream", mWorkerId);
        mFinishLatch.countDown();
      }
    };
  }

  /**
   * Manages the logic of registering with the master in a stream.
   *
   */
  public void registerWithMaster() throws CancelledException, InternalException,
      DeadlineExceededException, InterruptedException {
    // onNext() - send the request batches
    // onError() - when an error occurs on the worker side, propagate the error status to
    //             the master side and then close on the worker side, the master will
    //             handle necessary cleanup on its side
    // onCompleted() - complete on the client side when all the batches are sent
    //                 and all ACKs are received
    mWorkerRequestObserver = mAsyncClient
        .withDeadlineAfter(mDeadlineMs, TimeUnit.MILLISECONDS)
        .registerWorkerStream(mMasterResponseObserver);
    try {
      registerInternal();
    } catch (DeadlineExceededException | InterruptedException | CancelledException e) {
      LOG.error("Worker {} - Error during the register stream, aborting now.", mWorkerId, e);
      // These exceptions are internal to the worker
      // Propagate to the master side so it can clean up properly
      mWorkerRequestObserver.onError(e);
      throw e;
    }
    // The only exception that is not propagated to the master is InternalException.
    // We assume that is from the master so there is no need to send it back again.
  }

  private void registerInternal() throws InterruptedException, DeadlineExceededException,
      CancelledException, InternalException {
    int iter = 0;
    while (hasNext()) {
      // Send a request when the master ACKs the previous one
      LOG.debug("Worker {} - Acquiring one token to send the next batch", mWorkerId);
      Instant start = Instant.now();
      if (!mBucket.tryAcquire(mResponseTimeoutMs, TimeUnit.MILLISECONDS)) {
        throw new DeadlineExceededException(
            String.format("No response from master for more than %dms during the stream!",
                mResponseTimeoutMs));
      }
      Instant end = Instant.now();
      LOG.debug("Worker {} - master ACK received in {}ms, sending the next batch {}",
          mWorkerId, Duration.between(start, end).toMillis(), iter);

      // Send the request
      RegisterWorkerPRequest request = next();
      mWorkerRequestObserver.onNext(request);

      if (mFinishLatch.getCount() == 0) {
        abort();
      }

      iter++;
    }

    // Wait for all batches have been ACK-ed by the master before completing the client side
    if (!mAckLatch.await(mResponseTimeoutMs * MAX_BATCHES_IN_FLIGHT, TimeUnit.MILLISECONDS)) {
      // If the master side is closed before the client side, there is a problem
      if (mFinishLatch.getCount() == 0) {
        abort();
      }

      long receivedCount = mBlockMapIterator.getBatchCount() - mAckLatch.getCount();
      throw new DeadlineExceededException(
          String.format("All batches have been sent to the master but only received %d ACKs!",
              receivedCount));
    }
    LOG.info("Worker {} - All requests have been sent. Completing the client side.", mWorkerId);
    mWorkerRequestObserver.onCompleted();
    LOG.info("Worker {} - Waiting on the master side to complete", mWorkerId);
    if (!mFinishLatch.await(mCompleteTimeoutMs, TimeUnit.MILLISECONDS)) {
      throw new DeadlineExceededException(
          String.format("All batches have been received by the master but the master failed"
              + " to complete the registration in %dms!", mCompleteTimeoutMs));
    }
    // If the master failed in completing the request, there will also be an error
    if (mError.get() != null) {
      Throwable t = mError.get();
      LOG.error("Worker {} - Received an error from the master on completion", mWorkerId, t);
      throw new InternalException(t);
    }
    LOG.info("Worker {} - Finished registration with a stream", mWorkerId);
  }

  private void abort() throws InternalException, CancelledException {
    if (mError.get() != null) {
      Throwable t = mError.get();
      LOG.error("Worker {} - Received an error from the master", mWorkerId, t);
      throw new InternalException(t);
    } else {
      String msg = String.format("Worker %s - The server side has been closed before "
          + "all the batches are sent from the worker!", mWorkerId);
      LOG.error(msg);
      throw new CancelledException(msg);
    }
  }

  @Override
  public boolean hasNext() {
    // There will be at least 1 request even if the blocks are empty
    return mBlockMapIterator.hasNext() || mBatchNumber == 0;
  }

  @Override
  public RegisterWorkerPRequest next() {
    RegisterWorkerPRequest request;
    List<LocationBlockIdListEntry> blockBatch;
    if (mBatchNumber == 0) {
      if (mBlockMapIterator.hasNext()) {
        blockBatch = mBlockMapIterator.next();
      } else {
        blockBatch = Collections.emptyList();
      }
      // If it is the 1st batch, include metadata
      request = RegisterWorkerPRequest.newBuilder()
          .setWorkerId(mWorkerId)
          .addAllStorageTiers(mStorageTierAliases)
          .putAllTotalBytesOnTiers(mTotalBytesOnTiers)
          .putAllUsedBytesOnTiers(mUsedBytesOnTiers)
          .putAllLostStorage(mLostStorageMap)
          .setOptions(mOptions)
          .addAllCurrentBlocks(blockBatch)
          .build();
    } else {
      blockBatch = mBlockMapIterator.next();
      // Following batches only include the block list
      request = RegisterWorkerPRequest.newBuilder()
          .setWorkerId(mWorkerId)
          .addAllCurrentBlocks(blockBatch)
          .build();
    }
    mBatchNumber++;
    return request;
  }
}
