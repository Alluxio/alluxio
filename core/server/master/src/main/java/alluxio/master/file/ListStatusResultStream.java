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

import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPResponse;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Used to define a single batch of listing.
 */
@ThreadSafe
public class ListStatusResultStream implements ResultStream<FileInfo> {
  /** List of file infos. */
  private final List<FileInfo> mInfos;
  /** Batch size. */
  private final int mBatchSize;
  /** Client-side gRPC stream observer. */
  private final StreamObserver<ListStatusPResponse> mClientObserver;
  /** Whether stream is still active. */
  private boolean mStreamActive = true;
  /** The context. */
  private final ListStatusContext mContext;

  /**
   * Creates a new result streamer for listStatus call.
   *
   * @param batchSize batch size
   * @param clientObserver client stream
   * @param context the list status context
   */
  public ListStatusResultStream(int batchSize, StreamObserver<ListStatusPResponse> clientObserver,
                                ListStatusContext context) {
    Preconditions.checkArgument(batchSize > 0);
    Objects.requireNonNull(context);
    mBatchSize = batchSize;
    mClientObserver = clientObserver;
    mInfos = new ArrayList<>();
    mContext = context;
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
    return ListStatusPResponse.newBuilder()
        .addAllFileInfos(
            mInfos.stream().map(GrpcUtils::toProto).collect(Collectors.toList()))
        .setIsTruncated(mContext.isTruncated())
        .build();
  }
}
