package alluxio.worker.block;

import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerStreamPOptions;
import alluxio.grpc.RegisterWorkerStreamPRequest;
import alluxio.grpc.RegisterWorkerStreamPResponse;
import alluxio.grpc.StorageList;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.esotericsoftware.minlog.Log.info;

public class RegisterStream {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStream.class);

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

  RegisterStream(final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub asyncClient,
                 final long workerId, final List<String> storageTierAliases,
                 final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
                 final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
                 final Map<String, List<String>> lostStorage,
                 final List<ConfigProperty> configList) {
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
        LOG.info("register got response {}", res);
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("register received error", t);
        LOG.info("Closing latch");
        mFinishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        LOG.info("Completed. Closing latch");
        mFinishLatch.countDown();
      }
    };

    mRequestObserver = asyncClient.registerWorkerStream(responseObserver);
  }

  // TODO(jiacheng): switch to this method
  void registerSync() throws InterruptedException {
    try {

      // TODO(jiacheng): Decide what blocks to include
      //  This is taking long to finish because of the copy in the constructor
      Iterator<List<LocationBlockIdListEntry>> blockListIterator = new BlockMapIterator(mCurrentBlocksOnLocation);

      // Some extra conversions
      final RegisterWorkerStreamPOptions options =
              RegisterWorkerStreamPOptions.newBuilder().addAllConfigs(mConfigList).build();
      final Map<String, StorageList> lostStorageMap = mLostStorage.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                      e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));

      int iter = 0;
      while (blockListIterator.hasNext()) {
        List<LocationBlockIdListEntry> blockBatch = blockListIterator.next();

        LOG.info("Sending batch {}: {}", iter, blockBatch);

        // Generate a request
        RegisterWorkerStreamPRequest request;

        // If it is the 1st request, include metadata
        System.out.format("Generating %s request%n", iter);
        if (iter == 0) {
          System.out.println("Generating header request of the stream");
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
          System.out.println("Generating stream chunks");
          request = RegisterWorkerStreamPRequest.newBuilder()
                  .setIsHead(false)
                  .setWorkerId(mWorkerId)
                  .addAllCurrentBlocks(blockBatch)
                  .build();
        }

        // Send the request
        mRequestObserver.onNext(request);

        // TODO(jiacheng): what to do here
        if (mFinishLatch.getCount() == 0) {
          // RPC completed or errored before we finished sending.
          // Sending further requests won't error, but they will just be thrown away.
          LOG.error("");
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
      mRequestObserver.onError(t);
      throw t;
    }
    // Mark the end of requests
    LOG.info("All requests have been sent. Completing the client side.");
    System.out.format("Completing stream%n");
    mRequestObserver.onCompleted();

    // Receiving happens asynchronously
    // TODO(jiacheng): configurable
    LOG.info("Waiting on the latch");
    mFinishLatch.await(1, TimeUnit.MINUTES);
    LOG.info("Latch returned");
  }

  // TODO(jiacheng): a call to complete or close?
}
