/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.block;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.annotation.PublicApi;
import tachyon.worker.NetAddress;

/**
 * Information of an active block worker.
 */
@PublicApi @ThreadSafe
public final class BlockWorkerInfo {
  private final NetAddress mNetAddress;
  private final long mCapacityBytes;
  private final long mUsedBytes;

  /**
   * Constructs the block worker information.
   *
   * @param netAddress the address of the worker
   * @param capacityBytes the capacity of the worker in bytes
   * @param usedBytes the used bytes of the worker
   */
  public BlockWorkerInfo(NetAddress netAddress, long capacityBytes, long usedBytes) {
    mNetAddress = Preconditions.checkNotNull(netAddress);
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  /**
   * @return the address of the worker
   */
  public NetAddress getNetAddress() {
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
}
