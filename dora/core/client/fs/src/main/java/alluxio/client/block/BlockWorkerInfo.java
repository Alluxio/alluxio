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

package alluxio.client.block;

import alluxio.annotation.PublicApi;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Information of an active block worker.
 */
@PublicApi
@ThreadSafe
public final class BlockWorkerInfo {
  private final WorkerNetAddress mNetAddress;
  private final long mCapacityBytes;
  private final long mUsedBytes;

  /**
   * Constructs the block worker information.
   *
   * @param netAddress the address of the worker
   * @param capacityBytes the capacity of the worker in bytes
   * @param usedBytes the used bytes of the worker
   */
  public BlockWorkerInfo(WorkerNetAddress netAddress, long capacityBytes, long usedBytes) {
    mNetAddress = Preconditions.checkNotNull(netAddress, "netAddress");
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  /**
   * @return the address of the worker
   */
  public WorkerNetAddress getNetAddress() {
    return mNetAddress;
  }

  /**
   * @return the capacity of the worker in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the used bytes of the worker
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("netAddress", mNetAddress)
        .add("capacityBytes", mCapacityBytes)
        .add("usedBytes", mUsedBytes)
        .toString();
  }
}
