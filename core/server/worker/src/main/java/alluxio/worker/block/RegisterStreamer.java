package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.esotericsoftware.minlog.Log.info;

public class RegisterStreamer implements Iterator<RegisterWorkerPRequest> {
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

  StreamObserver<RegisterWorkerPRequest> mRequestObserver;
  final CountDownLatch mAckLatch;
  final CountDownLatch mFinishLatch;
  BlockMapIterator mBlockMapIterator;

  // How many batches active in a stream
  Semaphore mBucket = new Semaphore(2);
  AtomicReference<Throwable> mError = new AtomicReference<>();

  RegisterWorkerPOptions mOptions;
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
          BlockMapIterator blockListIterator) {
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
    mOptions = RegisterWorkerPOptions.newBuilder().addAllConfigs(mConfigList).build();
    mLostStorageMap = mLostStorage.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));
    mBatchNumber = 0;

    mBlockMapIterator = blockListIterator;
    mAckLatch = new CountDownLatch(mBlockMapIterator.getBatchCount());
  }

  public void registerWithMaster() throws InterruptedException, IOException {
    StreamObserver<RegisterWorkerPResponse> responseObserver = new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse res) {
        LOG.debug("{} - Received ACK {}", mWorkerId, res);
        mBucket.release();
        mAckLatch.countDown();
      }

      @Override
      public void onError(Throwable t) {
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
            .withDeadlineAfter(
                ServerConfiguration.getMs(PropertyKey.WORKER_REGISTER_STREAMING_DEADLINE), TimeUnit.MILLISECONDS)
            .registerWorkerStream(responseObserver);

    LOG.debug("{} - starting to register", mWorkerId);
    registerInternal();

    LOG.debug("{} - register finished", mWorkerId);
  }

  // TODO(jiacheng): what to do if deadlines are exceeded, retry?
  public void registerInternal() throws InterruptedException, IOException {
    try {
      int iter = 0;
      while (hasNext()) {
        // Send a request when the master ACKs the previous one
        LOG.debug("{} - Acquiring one token", mWorkerId);
        Instant start = Instant.now();
        // TODO(jiacheng): configurable deadline
        mBucket.acquire();
        Instant end = Instant.now();
        LOG.debug("{} - master ACK acquired in {}ms, sending the next iter {}",
                mWorkerId, Duration.between(start, end).toMillis(), iter);

        // Send the request
        RegisterWorkerPRequest request = next();
        mRequestObserver.onNext(request);

        if (mFinishLatch.getCount() == 0) {
          abort();
        }

        iter++;
      }
    } catch (InterruptedException | IOException e) {
      System.out.format("Error during stream %s%n", e);
      // Propagates the error to the server and close the channel
      mRequestObserver.onError(e);
      throw e;
    }

    // If the master side is closed before the client side, there is a problem
    if (mFinishLatch.getCount() == 0) {
      abort();
    }

    // Wait for all batches have been ACK-ed by the master before completing the client side
    // TODO(jiacheng): configurable deadline
    mAckLatch.await();
    LOG.info("{} - All requests have been sent. Completing the client side.", mWorkerId);
    mRequestObserver.onCompleted();
    LOG.info("{} - Waiting on the master side to complete", mWorkerId);
    // TODO(jiacheng): configurable deadline
    mFinishLatch.await();
    LOG.info("{} - Master completed", mWorkerId);

    if (mError != null) {
      LOG.error("Received error in register");
      throw new IOException(mError.get());
    }
  }

  // TODO(jiacheng): better name
  void abort() throws IOException {
    if (mError.get() != null) {
      Throwable t = mError.get();
      LOG.error("An error occurred during the stream", t);
      throw new IOException(t);
    } else {
      String msg = "The server side has been closed before all the batches are sent from the worker!";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  @Override
  public boolean hasNext() {
    return mBlockMapIterator.hasNext();
  }

  @Override
  public RegisterWorkerPRequest next() {
    RegisterWorkerPRequest request;
    List<LocationBlockIdListEntry> blockBatch = mBlockMapIterator.next();
    if (mBatchNumber == 0) {
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
