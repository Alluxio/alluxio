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

package alluxio.master.meta;

import alluxio.wire.Address;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Master information.
 */
@NotThreadSafe
public final class MasterInfo {
  /** Master's address. */
  private final Address mAddress;
  /** The id of the master. */
  private final long mId;
  /** Master's last updated time in ms. */
  private long mLastUpdatedTimeMs;
  /** Master's start time in ms. */
  private long mStartTimeMs = 0;
  /** Master's last lose primacy time in ms. */
  private long mLosePrimacyTimeMs = 0;
  /** Master's version. */
  private String mVersion = "";
  /** Master's revision. */
  private String mRevision = "";
  /** Master's last checkpoint time in ms. */
  private long mLastCheckpointTimeMs = 0;
  /** Number of journal entries since last checkpoint. */
  private long mJournalEntriesSinceCheckpoint = 0;

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
   * @return the start time of the master in ms
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the last lose primacy time of the master in ms
   */
  public long getLosePrimacyTimeMs() {
    return mLosePrimacyTimeMs;
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
   * @return the time of last checkpoint
   */
  public long getLastCheckpointTimeMs() {
    return mLastCheckpointTimeMs;
  }

  /**
   * @return number of journal entries since last checkpoint
   */
  public long getJournalEntriesSinceCheckpoint() {
    return mJournalEntriesSinceCheckpoint;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mId).add("address", mAddress)
        .add("lastUpdatedTimeMs", mLastUpdatedTimeMs).add("startTimeMs", mStartTimeMs)
        .add("losePrimacyTimeMs", mLosePrimacyTimeMs)
        .add("version", mVersion).add("revision", mRevision).toString();
  }

  /**
   * @param startTimeMs the start time of the master in ms
   */
  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  /**
   * @param losePrimacyTimeMs the last primacy state change time of the master in ms
   */
  public void setLosePrimacyTimeMs(long losePrimacyTimeMs) {
    mLosePrimacyTimeMs = losePrimacyTimeMs;
  }

  /**
   * @param version the version of the master
   */
  public void setVersion(String version) {
    mVersion = version;
  }

  /**
   * @param revision the revision of the master
   */
  public void setRevision(String revision) {
    mRevision = revision;
  }

  /**
   * Updates the last updated time of the master in ms.
   */
  public void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * @param lastCheckpointTimeMs the time of last checkpoint
   */
  public void setLastCheckpointTimeMs(long lastCheckpointTimeMs) {
    mLastCheckpointTimeMs = lastCheckpointTimeMs;
  }

  /**
   * @param journalEntriesSinceCheckpoint number of journal entries since last checkpoint
   */
  public void setJournalEntriesSinceCheckpoint(long journalEntriesSinceCheckpoint) {
    mJournalEntriesSinceCheckpoint = journalEntriesSinceCheckpoint;
  }
}
