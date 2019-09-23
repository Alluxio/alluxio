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
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to define a single batch of listing.
 */
public class ListStatusBatch {
  /** List of file infos. */
  private List<FileInfo> mInfos;
  /** Batch capacity. */
  private int mBatchCapacity;

  /**
   * Creates a new listing batch.
   *
   * @param batchCapacity capacity for the batch
   */
  public ListStatusBatch(int batchCapacity) {
    Preconditions.checkArgument(batchCapacity > 0);
    mBatchCapacity = batchCapacity;
    mInfos = new ArrayList<>(batchCapacity);
  }

  /**
   * Clears the batch content.
   */
  public void clear() {
    mInfos.clear();
  }

  /**
   * Adds a new item to batch.
   *
   * @param fileInfo file info item
   */
  public void add(FileInfo fileInfo) {
    mInfos.add(fileInfo);
  }

  /**
   * Removes the last item in the batch.
   *
   * @return returns the last item in the batch
   */
  public FileInfo pop() {
    return mInfos.remove(mInfos.size() - 1);
  }

  /**
   * Batch will be considered complete when item count reaches given capacity.
   *
   * @return whether the batch is complete
   */
  public boolean isComplete() {
    return mInfos.size() >= mBatchCapacity;
  }

  /**
   * @return item count for the batch
   */
  public int size() {
    return mInfos.size();
  }

  /**
   * @return the proto representation of listing batch
   */
  public ListStatusPResponse toProto() {
    return ListStatusPResponse.newBuilder()
        .addAllFileInfos(
            mInfos.stream().map((info) -> GrpcUtils.toProto(info)).collect(Collectors.toList()))
        .build();
  }

  /**
   * @return unmodifiable list of batch items
   */
  public List<FileInfo> toList() {
    return Collections.unmodifiableList(mInfos);
  }

  @Override
  public String toString() {
    return String.format("ListStatusBatch. Size:%d", mInfos.size());
  }
}
