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

package alluxio.master.file;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPResponse;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to define a single batch of listing.
 */
@ThreadSafe
public class ListStatusResultStream implements ResultStream<FileInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(ListStatusResultStream.class);
  private static final long RPC_RESPONSE_SIZE_WARNING_THRESHOLD =
      ServerConfiguration.getBytes(PropertyKey.MASTER_RPC_RESPONSE_SIZE_WARNING_THRESHOLD);

  /** List of file infos. */
  private List<FileInfo> mInfos;
  /** Batch size. */
  private int mBatchSize;
  /** Cliet-side gRPC stream observer. */
  private StreamObserver<ListStatusPResponse> mClientObserver;
  /** Whether stream is still active. */
  private boolean mStreamActive = true;

  /**
   * Creates a new result streamer for listStatus call.
   *
   * @param batchSize batch size
   * @param clientObserver client stream
   */
  public ListStatusResultStream(int batchSize, StreamObserver<ListStatusPResponse> clientObserver) {
    Preconditions.checkArgument(batchSize > 0);
    mBatchSize = batchSize;
    mClientObserver = clientObserver;
    mInfos = new ArrayList<>();
  }

  @Override
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
    ListStatusPResponse response = ListStatusPResponse.newBuilder()
        .addAllFileInfos(
            mInfos.stream().map((info) -> GrpcUtils.toProto(info)).collect(Collectors.toList()))
        .build();
    if (response.getSerializedSize() > RPC_RESPONSE_SIZE_WARNING_THRESHOLD) {
      LOG.warn("listStatus batch response is {} bytes, {} FileInfo",
          response.getSerializedSize(), mInfos.size());
    }
    return response;
  }
}
