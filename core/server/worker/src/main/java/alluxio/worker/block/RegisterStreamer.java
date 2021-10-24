package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

public class RegisterStreamer implements Iterator<RegisterWorkerPRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStreamer.class);

  private static final int MAX_BATCHES_IN_FLIGHT = 2;

  final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub mAsyncClient;

  final long mWorkerId;
  final List<String> mStorageTierAliases;
  final Map<String, Long> mTotalBytesOnTiers;
  final Map<String, Long> mUsedBytesOnTiers;
  final Map<BlockStoreLocation, List<Long>> mCurrentBlocksOnLocation;
  final Map<String, List<String>> mLostStorage;
  final List<ConfigProperty> mConfigList;
  RegisterWorkerPOptions mOptions;
  Map<String, StorageList> mLostStorageMap;

  int mBatchNumber;
  BlockMapIterator mBlockMapIterator;

  // For internal flow control and state mgmt
  final CountDownLatch mAckLatch;
  final CountDownLatch mFinishLatch;
  Semaphore mBucket = new Semaphore(MAX_BATCHES_IN_FLIGHT);
  AtomicReference<Throwable> mError = new AtomicReference<>();

  private final int mResponseTimeoutMs;
  private final int mDeadlineMs;
  private final int mCompleteTimeoutMs;

  public final StreamObserver<RegisterWorkerPResponse> mResponseObserver;
  public final StreamObserver<RegisterWorkerPRequest> mRequestObserver;

  public RegisterStreamer(
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
          final long workerId, final List<String> storageTierAliases,
          final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
          final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
          final Map<String, List<String>> lostStorage,
          final List<ConfigProperty> configList) {
    this(asyncClient, workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnLocation, lostStorage, configList,
            new BlockMapIterator(currentBlocksOnLocation));
  }

  @VisibleForTesting
  public RegisterStreamer(
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
          final long workerId, final List<String> storageTierAliases,
          final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
          final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
          final Map<String, List<String>> lostStorage,
          final List<ConfigProperty> configList,
          BlockMapIterator blockListIterator) {
    mAsyncClient = asyncClient;
    mWorkerId = workerId;
    mStorageTierAliases = storageTierAliases;
    mTotalBytesOnTiers = totalBytesOnTiers;
    mUsedBytesOnTiers = usedBytesOnTiers;
    mCurrentBlocksOnLocation = currentBlocksOnLocation;
    mLostStorage = lostStorage;
    mConfigList = configList;

    // Some extra conversions
    // TODO(jiacheng): remove unnecessary conversions
    mOptions = RegisterWorkerPOptions.newBuilder().addAllConfigs(mConfigList).build();
    mLostStorageMap = mLostStorage.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));
    mBatchNumber = 0;
    mBlockMapIterator = blockListIterator;

    mAckLatch = new CountDownLatch(mBlockMapIterator.getBatchCount());
    mFinishLatch = new CountDownLatch(1);

    mResponseTimeoutMs = (int) ServerConfiguration.getMs(PropertyKey.WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT);
    mDeadlineMs = (int) ServerConfiguration.getMs(PropertyKey.WORKER_REGISTER_STREAM_DEADLINE);
    mCompleteTimeoutMs = (int) ServerConfiguration.getMs(PropertyKey.WORKER_REGISTER_STREAM_COMPLETE_TIMEOUT);

    mResponseObserver = new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse res) {
        LOG.debug("{} - Received ACK {}", mWorkerId, res);
        mBucket.release();
        mAckLatch.countDown();
      }

      @Override
      public void onError(Throwable t) {
        System.out.println("Received error from master " + t);
        LOG.error("register received error from server, closing latch: ", t);
        mError.set(t);
        mFinishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        LOG.info("{} - Complete message received from the server. Closing stream", mWorkerId);
        mFinishLatch.countDown();
      }
    };

    // onNext() - send the request batches
    // onError() - when an error occurs on the worker side, propagate the error status to the master
    //             side and then close on the worker side, the master will handle necessary cleanup on its side
    // onCompleted() - complete on the client side when all the batches are sent and all ACKs are received
    mRequestObserver = mAsyncClient
        .withDeadlineAfter(mDeadlineMs, TimeUnit.MILLISECONDS)
        .registerWorkerStream(mResponseObserver);
  }


  public void registerWithMaster()
      throws CancelledException, InternalException, DeadlineExceededException, InterruptedException {
    try {
      registerWithInternal();
    } catch (DeadlineExceededException | InterruptedException | CancelledException e) {
      LOG.error("Error during the register stream, aborting now.", e);
      // These exceptions are internal to the worker
      // Propagate to the master side so it can clean up properly
      mRequestObserver.onError(e);
      throw e;
    }
    // The only exception that is not propagated to the master is InternalException.
    // We assume that is from the master so there is no need to send it back again.
  }

  private void registerWithInternal()
      throws InterruptedException, DeadlineExceededException, CancelledException, InternalException {
    int iter = 0;
    while (hasNext()) {
      // Send a request when the master ACKs the previous one
      LOG.debug("{} - Acquiring one token", mWorkerId);
      Instant start = Instant.now();
      System.out.println("Acquiring the token now " + start + " tokens free: " + mBucket.availablePermits());
      if (!mBucket.tryAcquire(mResponseTimeoutMs, TimeUnit.MILLISECONDS)) {
        throw new DeadlineExceededException(
            String.format("No response from master for more than %dms during the stream!",
                mResponseTimeoutMs));
      }
      Instant end = Instant.now();
      System.out.format("%d - master ACK received in %dms, sending the next iter %d%n",
              mWorkerId, Duration.between(start, end).toMillis(), iter);
      LOG.debug("{} - master ACK received in {}ms, sending the next iter {}",
              mWorkerId, Duration.between(start, end).toMillis(), iter);

      // Send the request
      RegisterWorkerPRequest request = next();
      mRequestObserver.onNext(request);

      if (mFinishLatch.getCount() == 0) {
        abort();
      }

      iter++;
    }

    // If the master side is closed before the client side, there is a problem
    if (mFinishLatch.getCount() == 0) {
      abort();
    }

    // Wait for all batches have been ACK-ed by the master before completing the client side
    if (!mAckLatch.await(mResponseTimeoutMs * MAX_BATCHES_IN_FLIGHT, TimeUnit.MILLISECONDS)) {
      long receivedCount = mBlockMapIterator.getBatchCount() - mAckLatch.getCount();
      throw new DeadlineExceededException(
          String.format("All batches have been sent to the master but only received %d ACKs!",
              receivedCount));
    }
    LOG.info("{} - All requests have been sent. Completing the client side.", mWorkerId);
    mRequestObserver.onCompleted();
    LOG.info("{} - Waiting on the master side to complete", mWorkerId);
    if (!mFinishLatch.await(mCompleteTimeoutMs, TimeUnit.MILLISECONDS)) {
      throw new DeadlineExceededException(
          String.format("All batches have been received by the master but the master failed to complete the "
              + "registration in %dms!",
          mCompleteTimeoutMs));
    }
    // If the master failed in completing the request, there will also be an error
    if (mError.get() != null) {
      Throwable t = mError.get();
      LOG.error("Received an error from the master on completion", t);
      throw new InternalException(t);
    }
    LOG.info("{} - Finished registration with a stream", mWorkerId);
  }

  private void abort() throws InternalException, CancelledException {
    if (mError.get() != null) {
      Throwable t = mError.get();
      LOG.error("Received an error from the master", t);
      throw new InternalException(t);
    } else {
      String msg = "The server side has been closed before all the batches are sent from the worker!";
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

  public int getBlockCount() {
    return mBlockMapIterator.getBlockCount();
  }
}
