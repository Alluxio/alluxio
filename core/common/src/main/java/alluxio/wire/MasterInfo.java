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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Master information.
 */
@NotThreadSafe
public final class MasterInfo {
  /** Master's address. */
  private Address mAddress;
  /** The id of the master. */
  private long mId;
  /** Master's last updated time in ms. */
  private long mLastUpdatedTimeMs;

  /**
   * Creates a new instance of {@link MasterInfo}.
   */
  public MasterInfo() {}

  /**
   * Creates a new instance of {@link MasterInfo}.
   *
   * @param id the master id to use
   * @param address the master address to use
   */
  public MasterInfo(long id, Address address) {
    mAddress = Preconditions.checkNotNull(address, "address");
    mId = id;
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Creates a new instance of {@link MasterInfo}.
   *
   * @param masterInfo the masterInfo need to be cloned
   * @return the the master information
   */
  public MasterInfo clone(MasterInfo masterInfo) {
    mAddress = masterInfo.getAddress();
    mId = masterInfo.getId();
    mLastUpdatedTimeMs = masterInfo.getLastUpdatedTimeMs();
    return this;
  }

  /**
   * @return the master's address
   */
  public Address getAddress() {
    return mAddress;
  }

  /**
   * @return the id of the master
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the master in ms
   */
  public long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * @param address the master address information
   * @return the master information
   */
  public MasterInfo setAddress(Address address) {
    mAddress = address;
    return this;
  }

  /**
   * @param id the master id
   * @return the master information
   */
  public MasterInfo setId(long id) {
    mId = id;
    return this;
  }

  /**
   * @param lastUpdatedTimeMs the last update time in ms
   * @return the master information
   */
  public MasterInfo setLastUpdatedTimeMs(long lastUpdatedTimeMs) {
    mLastUpdatedTimeMs = lastUpdatedTimeMs;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mId).add("address", mAddress)
            .add("lastUpdatedTimeMs", mLastUpdatedTimeMs).toString();
  }

  /**
   * Updates the last updated time of the master (in milliseconds).
   */
  public void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MasterInfo)) {
      return false;
    }
    MasterInfo that = (MasterInfo) o;
    return mId == that.mId && Objects.equal(mAddress, that.mAddress)
            && mLastUpdatedTimeMs == that.mLastUpdatedTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mAddress, mLastUpdatedTimeMs);
  }
}
