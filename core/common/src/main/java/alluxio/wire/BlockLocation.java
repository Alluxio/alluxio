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

import alluxio.annotation.PublicApi;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The location of a block.
 */
@PublicApi
@NotThreadSafe
public final class BlockLocation implements Serializable {
  private static final long serialVersionUID = 9017017197104411532L;

  private long mWorkerId;
  private WorkerNetAddress mWorkerAddress = new WorkerNetAddress();
  private String mTierAlias = "";
  private String mMediumType = "";

  /**
   * Creates a new instance of {@link BlockLocation}.
   */
  public BlockLocation() {}

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
   * @return the medium type
   */
  public String getMediumType() {
    return mMediumType;
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
    Preconditions.checkNotNull(workerAddress, "workerAddress");
    mWorkerAddress = workerAddress;
    return this;
  }

  /**
   * @param tierAlias the tier alias to use
   * @return the block location
   */
  public BlockLocation setTierAlias(String tierAlias) {
    Preconditions.checkNotNull(tierAlias, "tierAlias");
    mTierAlias = tierAlias;
    return this;
  }

  /**
   *
   * @param mediumType the medium type to use
   * @return the block location
   */
  public BlockLocation setMediumType(String mediumType) {
    Preconditions.checkNotNull(mediumType, "mediumType");
    mMediumType = mediumType;
    return this;
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
        && mTierAlias.equals(that.mTierAlias) && mMediumType.equals(that.mMediumType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerId, mWorkerAddress, mTierAlias, mMediumType);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerId", mWorkerId)
        .add("address", mWorkerAddress)
        .add("tierAlias", mTierAlias)
        .add("mediumType", mMediumType)
        .toString();
  }
}
