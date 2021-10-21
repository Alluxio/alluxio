package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerStreamPOptions;
import alluxio.grpc.RegisterWorkerStreamPRequest;
import alluxio.grpc.RegisterWorkerStreamPResponse;
import alluxio.grpc.StorageList;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.esotericsoftware.minlog.Log.info;

public class RegisterStreamer implements Iterator<RegisterWorkerStreamPRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStreamer.class);
  final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceBlockingStub mClient;
  final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub mAsyncClient;

  final long mWorkerId;
  final List<String> mStorageTierAliases;
  final Map<String, Long> mTotalBytesOnTiers;
  final Map<String, Long> mUsedBytesOnTiers;
  final Map<BlockStoreLocation, List<Long>> mCurrentBlocksOnLocation;
  final Map<String, List<String>> mLostStorage;
  final List<ConfigProperty> mConfigList;

  StreamObserver<RegisterWorkerStreamPRequest> mRequestObserver;
  final CountDownLatch mFinishLatch;
  Iterator<List<LocationBlockIdListEntry>> mBlockListIterator;

  // How many batches active in a stream
  Semaphore mBucket = new Semaphore(2);
  Throwable mError = null;

  RegisterWorkerStreamPOptions mOptions;
  Map<String, StorageList> mLostStorageMap;
  int mBatchNumber;

  public RegisterStreamer(
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceBlockingStub client,
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
          final long workerId, final List<String> storageTierAliases,
          final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
          final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
          final Map<String, List<String>> lostStorage,
          final List<ConfigProperty> configList) {
    this(client, asyncClient, workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnLocation, lostStorage, configList,
            new BlockMapIterator(currentBlocksOnLocation));
  }

  @VisibleForTesting
  public RegisterStreamer(
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceBlockingStub client,
          final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
          final long workerId, final List<String> storageTierAliases,
          final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
          final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
          final Map<String, List<String>> lostStorage,
          final List<ConfigProperty> configList,
          Iterator<List<LocationBlockIdListEntry>> blockListIterator) {
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

    // Some extra conversions
    mOptions = RegisterWorkerStreamPOptions.newBuilder().addAllConfigs(mConfigList).build();
    mLostStorageMap = mLostStorage.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));
    mBatchNumber = 0;

    mBlockListIterator = blockListIterator;
  }

  public void registerSync() throws InterruptedException, IOException {
    StreamObserver<RegisterWorkerStreamPResponse> responseObserver = new StreamObserver<RegisterWorkerStreamPResponse>() {
      @Override
      public void onNext(RegisterWorkerStreamPResponse res) {
        LOG.debug("Received response {}", res);
        mBucket.release();
        LOG.debug("{} - batch finished, 1 token released", mWorkerId);
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("register received error from server, closing latch: ", t);
        mError = t;
        mFinishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        LOG.info("{} - Complete message received from the server. Closing stream", mWorkerId);
        mFinishLatch.countDown();
      }
    };
    mRequestObserver = mAsyncClient
            .withDeadlineAfter(
                ServerConfiguration.getMs(PropertyKey.WORKER_REGISTER_STREAMING_DEADLINE), TimeUnit.MILLISECONDS)
            .registerWorkerStream(responseObserver);

    LOG.debug("{} - starting to register", mWorkerId);
    registerInternal();

    LOG.debug("{} - register finished", mWorkerId);
  }

  public void registerInternal() throws InterruptedException, IOException {
    try {
      int iter = 0;
      while (hasNext()) {
        // Send a request when the master ACKs the previous one
        LOG.debug("{} - Acquiring one token", mWorkerId);
        Instant start = Instant.now();
        mBucket.acquire();
        Instant end = Instant.now();
        LOG.debug("{} - master ACK acquired in {}ms, sending batch {}",
                mWorkerId, Duration.between(start, end).toMillis(), iter);

        // Send the request
        RegisterWorkerStreamPRequest request = next();
        mRequestObserver.onNext(request);

        // TODO(jiacheng): what to do here, mark as failed and restart?
        if (mFinishLatch.getCount() == 0) {
          // RPC completed or errored before we finished sending.
          // Sending further requests won't error, but they will just be thrown away.
          LOG.error("The stream has not finished but the response has seen onError or onComplete");
          return;
        }

        iter++;
      }
    } catch (Throwable t) {
      // TODO(jiacheng): throwable? close the stream?
      // Cancel RPC
      System.out.format("Error %s%n", t);
      mRequestObserver.onError(t);
      throw t;
    }
    long threadId = Thread.currentThread().getId();
    // Mark the end of requests
    LOG.info("{} All requests have been sent. Completing the client side.", threadId);
    mRequestObserver.onCompleted();

    // Receiving happens asynchronously
    LOG.info("{} - Waiting on the latch", threadId);
    // TODO(jiacheng): configure the deadline
    mFinishLatch.await();
    LOG.info("{} - Latch returned", threadId);

    if (mError != null) {
      LOG.error("Received error in register");
      throw new IOException(mError);
    }
  }

  @Override
  public boolean hasNext() {
    return mBlockListIterator.hasNext();
  }

  @Override
  public RegisterWorkerStreamPRequest next() {
    // If it is the 1st request, include metadata
    RegisterWorkerStreamPRequest request;
    List<LocationBlockIdListEntry> blockBatch = mBlockListIterator.next();
    if (mBatchNumber == 0) {
      request = RegisterWorkerStreamPRequest.newBuilder()
              .setIsHead(true)
              .setWorkerId(mWorkerId)
              .addAllStorageTiers(mStorageTierAliases)
              .putAllTotalBytesOnTiers(mTotalBytesOnTiers)
              .putAllUsedBytesOnTiers(mUsedBytesOnTiers)
              .putAllLostStorage(mLostStorageMap)
              .setOptions(mOptions)
              .addAllCurrentBlocks(blockBatch)
              .build();
    } else {
      request = RegisterWorkerStreamPRequest.newBuilder()
              .setIsHead(false)
              .setWorkerId(mWorkerId)
              .addAllCurrentBlocks(blockBatch)
              .build();
    }
    mBatchNumber++;
    return request;
  }
}
