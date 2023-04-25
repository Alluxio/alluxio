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

package alluxio.master.block.meta;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The location of an Alluxio block.
 */
@ThreadSafe
public final class MasterBlockLocation {
  /** The id of the Alluxio worker. */
  private final long mWorkerId;
  /** The tier alias that the block is on in this worker. */
  private final String mTierAlias;

  /**
   * Creates a new instance of {@link MasterBlockLocation}.
   *
   * @param workerId the worker id to use
   * @param tierAlias the tier alias to use
   */
  MasterBlockLocation(long workerId, String tierAlias) {
    mWorkerId = workerId;
    mTierAlias = tierAlias;
  }

  /**
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * @return the alias of the tier that the block is on in this worker
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MasterBlockLocation)) {
      return false;
    }
    MasterBlockLocation that = (MasterBlockLocation) o;
    return Objects.equal(mWorkerId, that.mWorkerId)
        && Objects.equal(mTierAlias, that.mTierAlias);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerId, mTierAlias);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerId", mWorkerId)
        .add("tierAlias", mTierAlias)
        .toString();
  }
}
