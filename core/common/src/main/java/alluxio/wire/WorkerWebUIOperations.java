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
  private long mRpcQueueLength;

  /**
   * Creates a new instance of {@link WorkerWebUIInit}.
   */
  public WorkerWebUIOperations() {
  }

  /**
   * Gets the operation count.
   * @return the number of operations
   */
  public long getOperationCount() {
    return mOperationCount;
  }

  /**
   * Gets the current RPC queue length.
   * @return the RPC queue length
   */
  public long getRpcQueueLength() {
    return mRpcQueueLength;
  }

  /**
   * Sets the operation count.
   * @param operationCount the operation count
   * @return the current obj
   */
  public WorkerWebUIOperations setOperationCount(long operationCount) {
    mOperationCount = operationCount;
    return this;
  }

  /**
   * Sets the RPC queue length.
   * @param rpcQueueLength queue length
   * @return the current obj
   */
  public WorkerWebUIOperations setRpcQueueLength(long rpcQueueLength) {
    mRpcQueueLength = rpcQueueLength;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("operationCount", mOperationCount)
        .add("rpcQueueLength", mRpcQueueLength)
        .toString();
  }
}
