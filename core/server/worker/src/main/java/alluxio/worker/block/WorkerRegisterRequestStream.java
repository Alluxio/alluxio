package alluxio.worker.block;

import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.wire.FileInfo;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WorkerRegisterRequestStream {
  // TODO(jiacheng): use WorkerMeta and WorkerUsageMeta? This need extra import
//  int mWorkerId;


//  /** the id of the worker */
//  optional int64 workerId = 1;
//  /** the list of storage tiers */
//  repeated string storageTiers = 2;
//  /** the map of total bytes on each tier */
//  map<string, int64> totalBytesOnTiers = 3;
//  /** the map of used bytes on each tier */
//  map<string, int64> usedBytesOnTiers = 4;
//  /** the map of list of blocks on each tier (deprecated since 2.0, replaced by currentBlocks*/
//  map<string, TierList> currentBlocksOnTiers = 5;
//  optional RegisterWorkerPOptions options = 6;
//  /** the map of tier alias to a list of lost storage paths. */
//  map<string, StorageList> lostStorage = 7;
//  /** use repeated fields to represent mapping from BlockStoreLocationProto to TierList */
//  repeated LocationBlockIdListEntry currentBlocks = 8;

  List<Long> mBlocks;
  WorkerRegisterRequestStream(List<Long> blockIds) {
    mBlocks = blockIds;
  }

  /** List of file infos. */
  private List<FileInfo> mInfos;
  /** Batch size. */
  private int mBatchSize;
  /** Cliet-side gRPC stream observer. */
  private StreamObserver<RegisterWorkerPResponse> mClientObserver;
  /** Whether stream is still active. */
  private boolean mStreamActive = true;

  /**
   * Creates a new result streamer for listStatus call.
   *
   * @param batchSize batch size
   * @param clientObserver client stream
   */
  public WorkerRegisterRequestStream(int batchSize, StreamObserver<RegisterWorkerPResponse> clientObserver) {
    Preconditions.checkArgument(batchSize > 0);
    mBatchSize = batchSize;
    mClientObserver = clientObserver;
    mInfos = new ArrayList<>();
  }

  public WorkerRegisterRequestStream(int batchSize, List<Long> blocks, StreamObserver<RegisterWorkerPResponse> clientObserver) {
    Preconditions.checkArgument(batchSize > 0);
    mBatchSize = batchSize;
    mClientObserver = clientObserver;
    mInfos = new ArrayList<>();
  }

  @Override
  // TODO(jiacheng): common interface?
  public synchronized void submit(FileInfo item) {
    mInfos.add(item);
    if (mInfos.size() >= mBatchSize) {
      sendCurrentBatch();
    }
  }

  /**
   * Sends the current batch if there are any items.
   */
  private void sendCurrentBatch() {
    if (mInfos.size() > 0) {
      mClientObserver.onNext(toProto());
      mInfos.clear();
    }
  }

  /**
   * Used to complete the stream.
   * It sends any remaining items and closes the underlying stream.
   */
  public synchronized void complete() {
    if (!mStreamActive) {
      return;
    }
    try {
      sendCurrentBatch();
      mClientObserver.onCompleted();
    } finally {
      mStreamActive = false;
    }
  }

  /**
   * Used to fail streaming with an error.
   *
   * @param error streaming error
   */
  public synchronized void fail(Throwable error) {
    if (mStreamActive) {
      try {
        mClientObserver.onError(error);
      } finally {
        mStreamActive = false;
      }
    }
  }

  /**
   * @return the proto representation of currently batched items
   */
  private ListStatusPResponse toProto() {
    return ListStatusPResponse.newBuilder()
            .addAllFileInfos(
                    mInfos.stream().map((info) -> GrpcUtils.toProto(info)).collect(Collectors.toList()))
            .build();
  }

}
