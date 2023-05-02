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
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.grpc.ListStatusPartialPResponse;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.wire.FileInfo;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Used to track the results of a call to a partial listing.
 */
public class ListStatusPartialResultStream implements ResultStream<FileInfo> {
  private final List<alluxio.grpc.FileInfo> mInfos;
  private final StreamObserver<ListStatusPartialPResponse> mClientObserver;
  private final ListStatusContext mContext;

  /**
   * Create a result stream for a partial listing.
   * @param observer the response observer
   * @param context the listing context
   */
  public ListStatusPartialResultStream(
      StreamObserver<ListStatusPartialPResponse> observer, ListStatusContext context) {
    mClientObserver = Objects.requireNonNull(observer);
    mContext = Objects.requireNonNull(context);
    ListStatusPartialPOptions.Builder options = context.getPartialOptions().orElseThrow(() ->
        new RuntimeException("Expected partial options"));
    mInfos = options.hasBatchSize() ? new ArrayList<>(options.getBatchSize())
        : new ArrayList<>();
  }

  @Override
  public void submit(FileInfo item) {
    mInfos.add(GrpcUtils.toProto(item));
  }

  /**
   * Called if an error occurs during a partial listing.
   * @param t the error
   */
  public void onError(Throwable t) {
    mClientObserver.onError(t);
  }

  /**
   * Called once the listing is complete, and sends the response to the client.
   */
  public void complete() {
    mClientObserver.onNext(ListStatusPartialPResponse.newBuilder()
        .setFileCount(mContext.getTotalListings())
        .setIsTruncated(mContext.isTruncated())
        .addAllFileInfos(mInfos)
        .build());
    mClientObserver.onCompleted();
  }
}
