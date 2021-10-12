package alluxio.worker.block;

import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerStreamPOptions;
import alluxio.grpc.RegisterWorkerStreamPRequest;
import alluxio.grpc.RegisterWorkerStreamPResponse;
import alluxio.grpc.StorageList;
import alluxio.util.CommonUtils;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.esotericsoftware.minlog.Log.info;

public class RegisterStream {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStream.class);
  final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceBlockingStub mClient;
  final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub mAsyncClient;

  final long mWorkerId;
  final List<String> mStorageTierAliases;
  final Map<String, Long> mTotalBytesOnTiers;
  final Map<String, Long> mUsedBytesOnTiers;
  final Map<BlockStoreLocation, List<Long>> mCurrentBlocksOnLocation;
  final Map<String, List<String>> mLostStorage;
  final List<ConfigProperty> mConfigList;

  final StreamObserver<RegisterWorkerStreamPRequest> mRequestObserver;
  final CountDownLatch mFinishLatch;
  Iterator<List<LocationBlockIdListEntry>> mBlockListIterator;

  // How many batches active in a stream
  Semaphore mBucket = new Semaphore(1);
  Throwable mError = null;

  public RegisterStream(
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceBlockingStub client,
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
          final long workerId, final List<String> storageTierAliases,
          final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
          final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
          final Map<String, List<String>> lostStorage,
          final List<ConfigProperty> configList) {
    mClient = client;
    mAsyncClient = asyncClient;
    mWorkerId = workerId;
    mStorageTierAliases = storageTierAliases;
    mTotalBytesOnTiers = totalBytesOnTiers;
    mUsedBytesOnTiers = usedBytesOnTiers;
    mCurrentBlocksOnLocation = currentBlocksOnLocation;
    mLostStorage = lostStorage;
    mConfigList = configList;


    // Initialize the observer
    mFinishLatch = new CountDownLatch(1);
    StreamObserver<RegisterWorkerStreamPResponse> responseObserver = new StreamObserver<RegisterWorkerStreamPResponse>() {
      @Override
      public void onNext(RegisterWorkerStreamPResponse res) {
        LOG.info("Received response {}", res);
        // TODO(jiacheng): If the master instructs the worker to wait， do this in a separate PR

        mBucket.release();
        LOG.info("{} - batch finished, 1 token released", mWorkerId);
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("register received error from server, closing latch: ", t);
        mError = t;
        mFinishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        LOG.info("{} - Complete message received from the server. Closing latch", mWorkerId);
        mFinishLatch.countDown();
      }
    };
    mRequestObserver = asyncClient.registerWorkerStream(responseObserver);
  }

  @VisibleForTesting
  public RegisterStream(
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceBlockingStub client,
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
          final long workerId, final List<String> storageTierAliases,
          final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
          final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
          final Map<String, List<String>> lostStorage,
          final List<ConfigProperty> configList,
          Iterator<List<LocationBlockIdListEntry>> blockListIterator) {
    this(client, asyncClient, workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnLocation, lostStorage, configList);
    mBlockListIterator = blockListIterator;
  }

  public void registerSync() throws InterruptedException, IOException {
    LOG.info("{} - starting to register", mWorkerId);
    registerInternal();

    LOG.info("{} - register finished", mWorkerId);
  }

  public void registerInternal() throws InterruptedException, IOException {
    try {
      // TODO(jiacheng): Decide what blocks to include
      //  This is taking long to finish because of the copy in the constructor
      if (mBlockListIterator == null) {
        LOG.info("Generate BlockMapIterator");
        mBlockListIterator = new BlockMapIterator(mCurrentBlocksOnLocation);
      } else {
        LOG.info("Using cached iterator");
      }

      // Some extra conversions
      final RegisterWorkerStreamPOptions options =
              RegisterWorkerStreamPOptions.newBuilder().addAllConfigs(mConfigList).build();
      final Map<String, StorageList> lostStorageMap = mLostStorage.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                      e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));

      int iter = 0;
      while (mBlockListIterator.hasNext()) {
        List<LocationBlockIdListEntry> blockBatch = mBlockListIterator.next();

        // TODO(jiacheng): debug output
        StringBuilder sb = new StringBuilder();
        for (LocationBlockIdListEntry e : blockBatch) {
          sb.append(String.format("%s, ", e.getKey()));
        }
        LOG.info("Sending batch {}: {}", iter, sb.toString());

        // Generate a request
        RegisterWorkerStreamPRequest request;

        LOG.info("{} - Acquiring one token", mWorkerId);
        // TODO(jiacheng): timeout?
        Instant start = Instant.now();
        mBucket.acquire();
        Instant end = Instant.now();
        LOG.info("{} - Token acquired in {}ms, sending next batch",
                mWorkerId, Duration.between(start, end).toMillis());

        // If it is the 1st request, include metadata
//        System.out.format("Generating request iter %s%n", iter);
        if (iter == 0) {
//          System.out.println("Generating header request of the stream");
          request = RegisterWorkerStreamPRequest.newBuilder()
                  .setIsHead(true)
                  .setWorkerId(mWorkerId)
                  .addAllStorageTiers(mStorageTierAliases)
                  .putAllTotalBytesOnTiers(mTotalBytesOnTiers)
                  .putAllUsedBytesOnTiers(mUsedBytesOnTiers)
                  .putAllLostStorage(lostStorageMap)
                  .setOptions(options)
                  .addAllCurrentBlocks(blockBatch)
                  .build();
        } else {
//          System.out.println("Generating stream chunks");
          request = RegisterWorkerStreamPRequest.newBuilder()
                  .setIsHead(false)
                  .setWorkerId(mWorkerId)
                  .addAllCurrentBlocks(blockBatch)
                  .build();
        }

        // Send the request
//        System.out.println("Sending request " + iter);
        mRequestObserver.onNext(request);

        // TODO(jiacheng): what to do here
        if (mFinishLatch.getCount() == 0) {
          // RPC completed or errored before we finished sending.
          // Sending further requests won't error, but they will just be thrown away.
          LOG.error("The stream has not finished but the response has seen onError or onComplete");
          System.out.format("The stream has not finished but the response has seen onError or onComplete");
          return;
        }

        iter++;
      }
    } catch (Throwable t) {
      // TODO(jiacheng): throwable?
      //  close the stream?
      // Cancel RPC
      System.out.format("Error %s%n", t);
      // TODO(jiacheng): calling its own onError?
      mRequestObserver.onError(t);
      throw t;
    }
    long threadId = Thread.currentThread().getId();
    // Mark the end of requests
    LOG.info("{} All requests have been sent. Completing the client side.", threadId);
    mRequestObserver.onCompleted();

    // Receiving happens asynchronously
    // TODO(jiacheng): configurable
    LOG.info("{} - Waiting on the latch", threadId);
    mFinishLatch.await();
    LOG.info("{} - Latch returned", threadId);

    if (mError != null) {
      LOG.error("Received error in register");
      throw new IOException(mError);
    }
  }
}