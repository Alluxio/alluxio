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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIOperations implements Serializable {
  private static final long serialVersionUID = 5444572986825500733L;

  private long mOperationCount;
  private long mClientCount;
  private long mRpcQueueLength;
  private long mReaderQueueLength;
  private long mWriterQueueLength;
  private long mSerializedQueueLength;

  /**
   * Creates a new instance of {@link WorkerWebUIInit}.
   */
  public WorkerWebUIOperations() {
  }

  public long getOperationCount() {
    return mOperationCount;
  }
  public long getClientCount() {
    return mClientCount;
  }

  public long getRpcQueueLength() {
    return mRpcQueueLength;
  }

  public long getReaderQueueLength() {
    return mReaderQueueLength;
  }

  public long getWriterQueueLength() {
    return mWriterQueueLength;
  }

  public WorkerWebUIOperations setOperationCount(long operationCount) {
    mOperationCount = operationCount;
    return this;
  }

  public WorkerWebUIOperations setClientCount(long clientCount) {
    mClientCount = clientCount;
    return this;
  }

  public WorkerWebUIOperations setRpcQueueLength(long rpcQueueLength) {
    mRpcQueueLength = rpcQueueLength;
    return this;
  }

  public WorkerWebUIOperations setReaderQueueLength(long readerQueueLength) {
    mReaderQueueLength = readerQueueLength;
    return this;
  }

  public WorkerWebUIOperations setWriterQueueLength(long writerQueueLength) {
    mWriterQueueLength = writerQueueLength;
    return this;
  }

  public WorkerWebUIOperations setSerializedQueueLength(long serializedQueueLength) {
    mSerializedQueueLength = serializedQueueLength;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("operationCount", mOperationCount)
            .add("clientCount", mClientCount)
            .add("rpcQueueLength", mRpcQueueLength)
            .add("readerQueueLength", mReaderQueueLength)
            .add("writerQueueLength", mWriterQueueLength)
            .add("serializedQueueLength", mSerializedQueueLength)
            .toString();
  }
}
