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

package tachyon.wire;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The location of a block.
 */
@NotThreadSafe
public final class BlockLocation implements WireType<tachyon.thrift.BlockLocation> {
  private long mWorkerId;
  private WorkerNetAddress mWorkerAddress = new WorkerNetAddress();
  private String mTierAlias = "";

  /**
   * Creates a new instance of {@BlockLocation}.
   */
  public BlockLocation() {}

  /**
   * Creates a new instance of {@link BlockLocation} from a thrift representation.
   *
   * @param blockLocation the thrift representation of a block location
   */
  public BlockLocation(tachyon.thrift.BlockLocation blockLocation) {
    mWorkerId = blockLocation.getWorkerId();
    mWorkerAddress = new WorkerNetAddress(blockLocation.getWorkerAddress());
    mTierAlias = blockLocation.getTierAlias();
  }

  /**
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * @return the worker address
   */
  public WorkerNetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  /**
   * @param workerId the worker id to use
   * @return the block location
   */
  public BlockLocation setWorkerId(long workerId) {
    mWorkerId = workerId;
    return this;
  }

  /**
   * @param workerAddress the worker address to use
   * @return the block location
   */
  public BlockLocation setWorkerAddress(WorkerNetAddress workerAddress) {
    Preconditions.checkNotNull(workerAddress);
    mWorkerAddress = workerAddress;
    return this;
  }

  /**
   * @param tierAlias the tier alias to use
   * @return the block location
   */
  public BlockLocation setTierAlias(String tierAlias) {
    Preconditions.checkNotNull(tierAlias);
    mTierAlias = tierAlias;
    return this;
  }

  /**
   * @return thrift representation of the block location
   */
  @Override
  public tachyon.thrift.BlockLocation toThrift() {
    return new tachyon.thrift.BlockLocation(mWorkerId, mWorkerAddress.toThrift(), mTierAlias);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockLocation)) {
      return false;
    }
    BlockLocation that = (BlockLocation) o;
    return mWorkerId == that.mWorkerId && mWorkerAddress.equals(that.mWorkerAddress)
        && mTierAlias.equals(that.mTierAlias);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerId, mWorkerAddress, mTierAlias);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("workerId", mWorkerId).add("address", mWorkerAddress)
        .add("tierAlias", mTierAlias).toString();
  }
}
