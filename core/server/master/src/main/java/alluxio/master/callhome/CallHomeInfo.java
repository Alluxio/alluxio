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

package alluxio.master.callhome;

import alluxio.wire.WorkerInfo;

import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * Call home information, can be encoded into JSON.
 */
public final class CallHomeInfo {
  private int mVersion;
  private String mClusterVersion;
  private boolean mFaultTolerant;
  private int mWorkerCount;
  private WorkerInfo[] mWorkerInfos;
  private int mLostWorkerCount;
  private WorkerInfo[] mLostWorkerInfos;
  private String mMasterAddress;
  private int mNumberOfPaths;
  private long mStartTime; // unix time, milliseconds
  private long mUptime; // milliseconds
  private String mUfsType;
  private long mUfsSize; // bytes
  private StorageTier[] mStorageTiers;

  /**
   * Creates a new instance of {@link CallHomeInfo}.
   */
  public CallHomeInfo() {}

  /**
   * @return the json format version
   */
  public int getVersion() {
    return mVersion;
  }

  /**
   * @return the cluster's start time in milliseconds
   */
  public long getStartTime() {
    return mStartTime;
  }

  /**
   * @return the cluster's uptime in milliseconds
   */
  public long getUptime() {
    return mUptime;
  }

  /**
   * @return the cluster's version
   */
  public String getClusterVersion() {
    return mClusterVersion;
  }

  /**
   * @return whether the cluster is in fault tolerant mode
   */
  public boolean getFaultTolerant() {
    return mFaultTolerant;
  }

  /**
   * @return the number of the workers
   */
  public int getWorkerCount() {
    return mWorkerCount;
  }

  /**
   * @return the live worker info
   */
  public WorkerInfo[] getWorkerInfos() {
    return mWorkerInfos;
  }

  /**
   * @return the number of lost workers
   */
  public int getLostWorkerCount() {
    return mLostWorkerCount;
  }

  /**
   * @return the live worker info
   */
  public WorkerInfo[] getLostWorkerInfos() {
    return mLostWorkerInfos;
  }

  /**
   * @return the master rpc address
   */
  public String getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * @return the number of paths in the file system
   */
  public long getNumberOfPaths() {
    return mNumberOfPaths;
  }

  /**
  /**
   * @return the under storage's type
   */
  public String getUfsType() {
    return mUfsType;
  }

  /**
   * @return the under storage's size in bytes
   */
  public long getUfsSize() {
    return mUfsSize;
  }

  /**
   * @return the storage tiers
   */
  public StorageTier[] getStorageTiers() {
    return mStorageTiers;
  }

  /**
   * @param version the json format version to use
   */
  public void setVersion(int version) {
    mVersion = version;
  }

  /**
   * @param startTime the start time (in milliseconds) to use
   */
  public void setStartTime(long startTime) {
    mStartTime = startTime;
  }

  /**
   * @param uptime the uptime (in milliseconds) to use
   */
  public void setUptime(long uptime) {
    mUptime = uptime;
  }

  /**
   * @param version the version to use
   */
  public void setClusterVersion(String version) {
    mClusterVersion = version;
  }

  /**
   * @param faultTolerant whether the cluster is in fault tolerant mode
   */
  public void setFaultTolerant(boolean faultTolerant) {
    mFaultTolerant = faultTolerant;
  }

  /**
   * @param n the number of workers to use
   */
  public void setWorkerCount(int n) {
    mWorkerCount = n;
  }

  /**
   * @param workers the live worker infos
   */
  public void setWorkerInfos(WorkerInfo[] workers) {
    mWorkerInfos = new WorkerInfo[workers.length];
    System.arraycopy(workers, 0, mWorkerInfos, 0, workers.length);
  }

  /**
   * @param n the number of lost workers
   */
  public void setLostWorkerCount(int n) {
    mLostWorkerCount = n;
  }

  /**
   * @param lostWorkers the live worker infos
   */
  public void setLostWorkerInfos(WorkerInfo[] lostWorkers) {
    mLostWorkerInfos = new WorkerInfo[lostWorkers.length];
    System.arraycopy(lostWorkers, 0, mLostWorkerInfos, 0, lostWorkers.length);
  }

  /**
   * @param masterAddress rpc address of primary master
   */
  public void setMasterAddress(String masterAddress) {
    mMasterAddress = masterAddress;
  }

  /**
   * @param n the number of paths in the file system
   */
  public void setNumberOfPaths(int n) {
    mNumberOfPaths = n;
  }

  /**
   * @param type the under storage's type to use
   */
  public void setUfsType(String type) {
    mUfsType = type;
  }

  /**
   * @param size the under storage's size (in bytes) to use
   */
  public void setUfsSize(long size) {
    mUfsSize = size;
  }

  /**
   * @param storageTiers the storage tiers to use
   */
  public void setStorageTiers(StorageTier[] storageTiers) {
    mStorageTiers = new StorageTier[storageTiers.length];
    System.arraycopy(storageTiers, 0, mStorageTiers, 0, storageTiers.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CallHomeInfo)) {
      return false;
    }
    CallHomeInfo that = (CallHomeInfo) o;
    return Objects.equal(mVersion, that.mVersion)
        && Objects.equal(mStartTime, that.mStartTime)
        && Objects.equal(mUptime, that.mUptime)
        && Objects.equal(mClusterVersion, that.mClusterVersion)
        && Objects.equal(mFaultTolerant, that.mFaultTolerant)
        && Objects.equal(mWorkerCount, that.mWorkerCount)
        && Arrays.equals(mWorkerInfos, that.mWorkerInfos)
        && Objects.equal(mLostWorkerCount, that.mLostWorkerCount)
        && Arrays.equals(mLostWorkerInfos, that.mLostWorkerInfos)
        && Objects.equal(mMasterAddress, that.mMasterAddress)
        && Objects.equal(mNumberOfPaths, that.mNumberOfPaths)
        && Objects.equal(mUfsType, that.mUfsType)
        && Objects.equal(mUfsSize, that.mUfsSize)
        && Arrays.equals(mStorageTiers, that.mStorageTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mVersion, mStartTime, mUptime, mClusterVersion,
        mFaultTolerant, mWorkerCount, mWorkerInfos, mLostWorkerCount, mLostWorkerInfos,
        mMasterAddress, mNumberOfPaths, mUfsType, mUfsSize, mStorageTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("version", mVersion)
        .add("start time", mStartTime)
        .add("uptime", mUptime)
        .add("version", mClusterVersion)
        .add("fault tolerant", mFaultTolerant)
        .add("worker count", mWorkerCount)
        .add("worker infos", mWorkerInfos)
        .add("lost worker count", mLostWorkerCount)
        .add("lost worker infos", mLostWorkerInfos)
        .add("master address", mMasterAddress)
        .add("num paths", mNumberOfPaths)
        .add("ufs type", mUfsType)
        .add("ufs size", mUfsSize)
        .add("storage tiers", mStorageTiers)
        .toString();
  }

  /**
   * Represents a tier in the tiered storage.
   */
  public static final class StorageTier {
    private String mAlias;
    private long mSize; // bytes
    private long mUsedSizeInBytes;

    /**
     * Creates a new instance of {@link StorageTier}.
     */
    public StorageTier() {}

    /**
     * @return the tier's alias
     */
    public String getAlias() {
      return mAlias;
    }

    /**
     * @return the tier's size in bytes
     */
    public long getSize() {
      return mSize;
    }

    /**
     * @return the tier's used size in bytes
     */
    public long getUsedSizeInBytes() {
      return mUsedSizeInBytes;
    }

    /**
     * @param alias the tier's alias to use
     */
    public void setAlias(String alias) {
      mAlias = alias;
    }

    /**
     * @param size the tier's size (in bytes) to use
     */
    public void setSize(long size) {
      mSize = size;
    }

    /**
     * @param size the tier's used size (in bytes)
     */
    public void setUsedSizeInBytes(long size) {
      mUsedSizeInBytes = size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StorageTier)) {
        return false;
      }
      StorageTier that = (StorageTier) o;
      return Objects.equal(mAlias, that.mAlias)
          && Objects.equal(mSize, that.mSize);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mAlias, mSize);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("alias", mAlias).add("size", mSize).toString();
    }
  }
}
