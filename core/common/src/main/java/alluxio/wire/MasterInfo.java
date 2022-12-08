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
  /** Master's start time. */
  private String mStartTime = "";
  /** Master's last updated time. */
  private String mLastUpdatedTime = "";

  /**
   * Creates a new instance of {@link MasterInfo}.
   */
  public MasterInfo() {}

  /**
   * Creates a new instance of {@link MasterInfo}.
   *
   * @param id the master id
   * @param address the master's address
   * @param lastUpdatedTime the master's last updated time
   * @param startTime the master's start time
   */
  public MasterInfo(long id, Address address, String lastUpdatedTime, String startTime) {
    mAddress = Preconditions.checkNotNull(address, "address");
    mId = id;
    mLastUpdatedTime = lastUpdatedTime;
    mStartTime = startTime;
  }

  /**
   * Creates a new instance of {@link MasterInfo}.
   *
   * @param id the master id to use
   * @param address the master address to use
   */
  public MasterInfo(long id, Address address) {
    mAddress = Preconditions.checkNotNull(address, "address");
    mId = id;
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
   * @return the last updated time of the master
   */
  public String getLastUpdatedTime() {
    return mLastUpdatedTime;
  }

  /**
   * @return the start time of the master
   */
  public String getStartTime() {
    return mStartTime;
  }

  /**
   * @param startTime the start time of the master
   * @return the master information
   */
  public MasterInfo setStartTime(String startTime) {
    mStartTime = startTime;
    return this;
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
   * @param lastUpdatedTime the last update time
   * @return the master information
   */
  public MasterInfo setLastUpdatedTime(String lastUpdatedTime) {
    mLastUpdatedTime = lastUpdatedTime;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mId).add("address", mAddress)
        .add("lastUpdatedTime", mLastUpdatedTime)
        .add("startTime", mStartTime).toString();
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
        && mLastUpdatedTime.equals(that.mLastUpdatedTime)
        && mStartTime.equals(that.mStartTime);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mAddress, mLastUpdatedTime, mStartTime);
  }
}
