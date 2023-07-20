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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Master information.
 */
@NotThreadSafe
public final class MasterInfo {
  private static final String NONE = "N/A";
  /** Master's address. */
  private Address mAddress;
  /** The id of the master. */
  private long mId;
  /** Master's start time. */
  private String mStartTime = NONE;
  /** Master's last gain primacy time. */
  private String mGainPrimacyTime = NONE;
  /** Master's last lose primacy time. */
  private String mLosePrimacyTime = NONE;
  /** Master's last updated time. */
  private String mLastUpdatedTime = NONE;
  /** Master's version. */
  private String mVersion = NONE;
  /** Master's revision. */
  private String mRevision = NONE;
  /** Master's last checkpoint time. */
  private String mLastCheckpointTime = NONE;
  /** Master's journal entries since last checkpoint. */
  private long mJournalEntriesSinceCheckpoint = 0;

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
   * @return the last gain primacy time of the master
   */
  public String getGainPrimacyTime() {
    return mGainPrimacyTime;
  }

  /**
   * @return the last lose primacy time of the master
   */
  public String getLosePrimacyTime() {
    return mLosePrimacyTime;
  }

  /**
   * @return the version of the master
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * @return the revision of the master
   */
  public String getRevision() {
    return mRevision;
  }

  /**
   * @return the last checkpoint time
   */
  public String getLastCheckpointTime() {
    return mLastCheckpointTime;
  }

  /**
   * @return journal entries since last checkpoint
   */
  public long getJournalEntriesSinceCheckpoint() {
    return mJournalEntriesSinceCheckpoint;
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

  /**
   * @param lastUpdatedTime the last update time in ms
   * @return the master information
   */
  public MasterInfo setLastUpdatedTimeMs(long lastUpdatedTime) {
    return this.setLastUpdatedTime(convertMsToDate(lastUpdatedTime));
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
   * @param startTime the start time of the master in ms
   * @return the master information
   */
  public MasterInfo setStartTimeMs(long startTime) {
    return this.setStartTime(convertMsToDate(startTime));
  }

  /**
   * @param gainPrimacyTime the last gain primacy time of the master
   * @return the master information
   */
  public MasterInfo setGainPrimacyTime(String gainPrimacyTime) {
    mGainPrimacyTime = gainPrimacyTime;
    return this;
  }

  /**
   * @param gainPrimacyTimeMs the last gain primacy time of the master in ms
   * @return the master information
   */
  public MasterInfo setGainPrimacyTimeMs(long gainPrimacyTimeMs) {
    return this.setGainPrimacyTime(convertMsToDate(gainPrimacyTimeMs));
  }

  /**
   * @param losePrimacyTime the last lose primacy time of the master
   * @return the master information
   */
  public MasterInfo setLosePrimacyTime(String losePrimacyTime) {
    mLosePrimacyTime = losePrimacyTime;
    return this;
  }

  /**
   * @param losePrimacyTimeMs the last lose primacy time of the master in ms
   * @return the master information
   */
  public MasterInfo setLosePrimacyTimeMs(long losePrimacyTimeMs) {
    return this.setLosePrimacyTime(convertMsToDate(losePrimacyTimeMs));
  }

  /**
   * @param version the version of the master
   * @return the master information
   */
  public MasterInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * @param revision the revision of the master
   * @return the master information
   */
  public MasterInfo setRevision(String revision) {
    mRevision = revision;
    return this;
  }

  /**
   * @param lastCheckpointTime the last checkpoint time
   * @return the master information
   */
  public MasterInfo setLastCheckpointTime(String lastCheckpointTime) {
    mLastCheckpointTime = lastCheckpointTime;
    return this;
  }

  /**
   * @param lastCheckpointTime the last checkpoint time in ms
   * @return the master information
   */
  public MasterInfo setLastCheckpointTimeMs(long lastCheckpointTime) {
    return this.setLastCheckpointTime(convertMsToDate(lastCheckpointTime));
  }

  /**
   * @param journalEntriesSinceCheckpoint journal entries since last checkpoint
   * @return the master information
   */
  public MasterInfo setJournalEntriesSinceCheckpoint(long journalEntriesSinceCheckpoint) {
    mJournalEntriesSinceCheckpoint = journalEntriesSinceCheckpoint;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mId).add("address", mAddress)
        .add("lastUpdatedTime", mLastUpdatedTime)
        .add("startTime", mStartTime)
        .add("gainPrimacyTime", mGainPrimacyTime)
        .add("losePrimacyTime", mLosePrimacyTime)
        .add("lastCheckpointTime", mLastCheckpointTime)
        .add("journalEntriesSinceCheckpoint", mJournalEntriesSinceCheckpoint)
        .add("version", mVersion)
        .add("revision", mRevision).toString();
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
        && mStartTime.equals(that.mStartTime)
        && mGainPrimacyTime.equals(that.mGainPrimacyTime)
        && mLosePrimacyTime.equals(that.mLosePrimacyTime)
        && mLastCheckpointTime.equals(that.mLastCheckpointTime)
        && mJournalEntriesSinceCheckpoint == that.mJournalEntriesSinceCheckpoint
        && mVersion.equals(that.mVersion)
        && mRevision.equals(that.mRevision);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mAddress, mLastUpdatedTime, mStartTime, mGainPrimacyTime,
        mLosePrimacyTime, mLastCheckpointTime, mJournalEntriesSinceCheckpoint,
        mVersion, mRevision);
  }

  private static String convertMsToDate(long timeMs) {
    if (timeMs <= 0) {
      return NONE;
    }
    return CommonUtils.convertMsToDate(timeMs,
        Configuration.getString(PropertyKey.USER_DATE_FORMAT_PATTERN));
  }
}
