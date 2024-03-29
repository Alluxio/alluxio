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

import alluxio.metrics.TimeSeries;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI metrics information.
 */
@NotThreadSafe
public final class MasterWebUIMetrics implements Serializable {
  private static final long serialVersionUID = -2078580961778090417L;

  private int mMasterCapacityFreePercentage;
  private int mMasterCapacityUsedPercentage;
  private int mMasterUnderfsCapacityFreePercentage;
  private int mMasterUnderfsCapacityUsedPercentage;
  private Map<String, Counter> mRpcInvocationMetrics;
  private Map<String, Map<String, Long>> mUfsOps;
  private Map<String, Map<String, Long>> mUfsOpsSaved;
  private Map<String, Metric> mOperationMetrics;
  private Map<String, String> mUfsReadSize;
  private Map<String, String> mUfsWriteSize;
  private List<TimeSeries> mTimeSeriesMetrics;
  private List<JournalDiskInfo> mJournalDiskMetrics;
  private String mJournalLastCheckpointTime;
  private long mJournalEntriesSinceCheckpoint;
  private String mCacheHit;
  private String mCacheMiss;
  private String mTotalBytesReadRemote;
  private String mTotalBytesReadRemoteThroughput;
  private String mTotalBytesReadDomainSocket;
  private String mTotalBytesReadDomainSocketThroughput;
  private String mTotalBytesReadUfs;
  private String mTotalBytesReadUfsThroughput;
  private String mTotalBytesWrittenRemote;
  private String mTotalBytesWrittenRemoteThroughput;
  private String mTotalBytesWrittenDomainSocket;
  private String mTotalBytesWrittenDomainSocketThroughput;
  private String mTotalBytesWrittenUfs;
  private String mTotalBytesWrittenUfsThroughput;

  /**
   * Creates a new instance of {@link MasterWebUIMetrics}.
   */
  public MasterWebUIMetrics() {
  }

  /**
   * Gets cache hit.
   *
   * @return the cache hit remote
   */
  public String getCacheHit() {
    return mCacheHit;
  }

  /**
   * Gets cache miss.
   *
   * @return the cache miss
   */
  public String getCacheMiss() {
    return mCacheMiss;
  }

  /**
   * Gets master capacity free percentage.
   *
   * @return the master capacity free percentage
   */
  public int getMasterCapacityFreePercentage() {
    return mMasterCapacityFreePercentage;
  }

  /**
   * Gets master capacity used percentage.
   *
   * @return the master capacity used percentage
   */
  public int getMasterCapacityUsedPercentage() {
    return mMasterCapacityUsedPercentage;
  }

  /**
   * Gets master underfs capacity free percentage.
   *
   * @return the master underfs capacity free percentage
   */
  public int getMasterUnderfsCapacityFreePercentage() {
    return mMasterUnderfsCapacityFreePercentage;
  }

  /**
   * Gets master underfs capacity used percentage.
   *
   * @return the master underfs capacity used percentage
   */
  public int getMasterUnderfsCapacityUsedPercentage() {
    return mMasterUnderfsCapacityUsedPercentage;
  }

  /**
   * Gets total bytes read from domain socket.
   *
   * @return the total bytes from domain socket
   */
  public String getTotalBytesReadDomainSocket() {
    return mTotalBytesReadDomainSocket;
  }

  /**
   * Gets total bytes read from domain socket throughput.
   *
   * @return the total bytes read from domain socket throughput
   */
  public String getTotalBytesReadDomainSocketThroughput() {
    return mTotalBytesReadDomainSocketThroughput;
  }

  /**
   * Gets total bytes read remote.
   *
   * @return the total bytes read remote
   */
  public String getTotalBytesReadRemote() {
    return mTotalBytesReadRemote;
  }

  /**
   * Gets total bytes read remote throughput.
   *
   * @return the total bytes read remote throughput
   */
  public String getTotalBytesReadRemoteThroughput() {
    return mTotalBytesReadRemoteThroughput;
  }

  /**
   * Gets total bytes read ufs.
   *
   * @return the total bytes read ufs
   */
  public String getTotalBytesReadUfs() {
    return mTotalBytesReadUfs;
  }

  /**
   * Gets total bytes read ufs throughput.
   *
   * @return the total bytes read ufs throughput
   */
  public String getTotalBytesReadUfsThroughput() {
    return mTotalBytesReadUfsThroughput;
  }

  /**
   * Gets total bytes written remote.
   *
   * @return the total bytes written remote
   */
  public String getTotalBytesWrittenRemote() {
    return mTotalBytesWrittenRemote;
  }

  /**
   * Gets total bytes written remote throughput.
   *
   * @return the total bytes written remote throughput
   */
  public String getTotalBytesWrittenRemoteThroughput() {
    return mTotalBytesWrittenRemoteThroughput;
  }

  /**
   * Gets total bytes written through domain socket.
   *
   * @return the total bytes written through domain socket
   */
  public String getTotalBytesWrittenDomainSocket() {
    return mTotalBytesWrittenDomainSocket;
  }

  /**
   * Gets total bytes written through domain socket throughput.
   *
   * @return the total bytes written through domain socket throughput
   */
  public String getTotalBytesWrittenDomainSocketThroughput() {
    return mTotalBytesWrittenDomainSocketThroughput;
  }

  /**
   * Gets total bytes written ufs.
   *
   * @return the total bytes written ufs
   */
  public String getTotalBytesWrittenUfs() {
    return mTotalBytesWrittenUfs;
  }

  /**
   * Gets total bytes written ufs throughput.
   *
   * @return the total bytes written ufs throughput
   */
  public String getTotalBytesWrittenUfsThroughput() {
    return mTotalBytesWrittenUfsThroughput;
  }

  /**
   * Gets ufs ops.
   *
   * @return the ufs ops
   */
  public Map<String, Map<String, Long>> getUfsOps() {
    return mUfsOps;
  }

  /**
   * Gets ufs ops saved.
   *
   * @return the ufs ops saved
   */
  public Map<String, Map<String, Long>> getUfsOpsSaved() {
    return mUfsOpsSaved;
  }

  /**
   * Gets ufs read size.
   *
   * @return the ufs read size
   */
  public Map<String, String> getUfsReadSize() {
    return mUfsReadSize;
  }

  /**
   * Gets ufs write size.
   *
   * @return the ufs write size
   */
  public Map<String, String> getUfsWriteSize() {
    return mUfsWriteSize;
  }

  /**
   * Gets operation metrics.
   *
   * @return the operation metrics
   */
  public Map<String, Metric> getOperationMetrics() {
    return mOperationMetrics;
  }

  /**
   * Gets rpc invocation metrics.
   *
   * @return the rpc invocation metrics
   */
  public Map<String, Counter> getRpcInvocationMetrics() {
    return mRpcInvocationMetrics;
  }

  /**
   * @return the time series metrics
   */
  public List<TimeSeries> getTimeSeriesMetrics() {
    return mTimeSeriesMetrics;
  }

  /**
   * @return the journal disk metrics
   */
  public List<JournalDiskInfo> getJournalDiskMetrics() {
    return mJournalDiskMetrics;
  }

  /**
   * @return the last journal checkpoint time
   */
  public String getJournalLastCheckpointTime() {
    return mJournalLastCheckpointTime;
  }

  /**
   * @return the last journal checkpoint time
   */
  public long getJournalEntriesSinceCheckpoint() {
    return mJournalEntriesSinceCheckpoint;
  }

  /**
   * Sets cache hit.
   *
   * @param CacheHitRemote the cache hit remote
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setCacheHit(String CacheHitRemote) {
    mCacheHit = CacheHitRemote;
    return this;
  }

  /**
   * Sets cache miss.
   *
   * @param CacheMiss the cache miss
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setCacheMiss(String CacheMiss) {
    mCacheMiss = CacheMiss;
    return this;
  }

  /**
   * Sets master capacity free percentage.
   *
   * @param MasterCapacityFreePercentage the master capacity free percentage
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setMasterCapacityFreePercentage(int MasterCapacityFreePercentage) {
    mMasterCapacityFreePercentage = MasterCapacityFreePercentage;
    return this;
  }

  /**
   * Sets master capacity used percentage.
   *
   * @param MasterCapacityUsedPercentage the master capacity used percentage
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setMasterCapacityUsedPercentage(int MasterCapacityUsedPercentage) {
    mMasterCapacityUsedPercentage = MasterCapacityUsedPercentage;
    return this;
  }

  /**
   * Sets master underfs capacity free percentage.
   *
   * @param MasterUnderfsCapacityFreePercentage the master underfs capacity free percentage
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setMasterUnderfsCapacityFreePercentage(
      int MasterUnderfsCapacityFreePercentage) {
    mMasterUnderfsCapacityFreePercentage = MasterUnderfsCapacityFreePercentage;
    return this;
  }

  /**
   * Sets master underfs capacity used percentage.
   *
   * @param MasterUnderfsCapacityUsedPercentage the master underfs capacity used percentage
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setMasterUnderfsCapacityUsedPercentage(
      int MasterUnderfsCapacityUsedPercentage) {
    mMasterUnderfsCapacityUsedPercentage = MasterUnderfsCapacityUsedPercentage;
    return this;
  }

  /**
   * Sets total bytes read from domain socket.
   *
   * @param TotalBytesReadDomainSocket the total bytes read from domain socket
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesReadDomainSocket(String TotalBytesReadDomainSocket) {
    mTotalBytesReadDomainSocket = TotalBytesReadDomainSocket;
    return this;
  }

  /**
   * Sets total bytes read domain socket throughput.
   *
   * @param TotalBytesReadDomainSocketThroughput the total bytes read domain socket throughput
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesReadDomainSocketThroughput(
      String TotalBytesReadDomainSocketThroughput) {
    mTotalBytesReadDomainSocketThroughput = TotalBytesReadDomainSocketThroughput;
    return this;
  }

  /**
   * Sets total bytes read remote.
   *
   * @param TotalBytesReadRemote the total bytes read remote
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesReadRemote(String TotalBytesReadRemote) {
    mTotalBytesReadRemote = TotalBytesReadRemote;
    return this;
  }

  /**
   * Sets total bytes read remote throughput.
   *
   * @param TotalBytesReadRemoteThroughput the total bytes read remote throughput
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesReadRemoteThroughput(
      String TotalBytesReadRemoteThroughput) {
    mTotalBytesReadRemoteThroughput = TotalBytesReadRemoteThroughput;
    return this;
  }

  /**
   * Sets total bytes read ufs.
   *
   * @param TotalBytesReadUfs the total bytes read ufs
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesReadUfs(String TotalBytesReadUfs) {
    mTotalBytesReadUfs = TotalBytesReadUfs;
    return this;
  }

  /**
   * Sets total bytes read ufs throughput.
   *
   * @param TotalBytesReadUfsThroughput the total bytes read ufs throughput
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesReadUfsThroughput(String TotalBytesReadUfsThroughput) {
    mTotalBytesReadUfsThroughput = TotalBytesReadUfsThroughput;
    return this;
  }

  /**
   * Sets total bytes written remote.
   *
   * @param TotalBytesWrittenRemote the total bytes written remote
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesWrittenRemote(String TotalBytesWrittenRemote) {
    mTotalBytesWrittenRemote = TotalBytesWrittenRemote;
    return this;
  }

  /**
   * Sets total bytes written remote throughput.
   *
   * @param TotalBytesWrittenRemoteThroughput the total bytes written remote throughput
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesWrittenRemoteThroughput(
      String TotalBytesWrittenRemoteThroughput) {
    mTotalBytesWrittenRemoteThroughput = TotalBytesWrittenRemoteThroughput;
    return this;
  }

  /**
   * Sets total bytes written through domain socket.
   *
   * @param TotalBytesWrittenDoaminSocket the total bytes written through domain socket
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesWrittenDomainSocket(String TotalBytesWrittenDoaminSocket) {
    mTotalBytesWrittenDomainSocket = TotalBytesWrittenDoaminSocket;
    return this;
  }

  /**
   * Sets total bytes written domain socket throughput.
   *
   * @param TotalBytesWrittenDoaminSocketThroughput the total bytes written domain socket throughput
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesWrittenDomainSocketThroughput(
      String TotalBytesWrittenDoaminSocketThroughput) {
    mTotalBytesWrittenDomainSocketThroughput = TotalBytesWrittenDoaminSocketThroughput;
    return this;
  }

  /**
   * Sets total bytes written ufs.
   *
   * @param TotalBytesWrittenUfs the total bytes written ufs
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesWrittenUfs(String TotalBytesWrittenUfs) {
    mTotalBytesWrittenUfs = TotalBytesWrittenUfs;
    return this;
  }

  /**
   * Sets total bytes written ufs throughput.
   *
   * @param TotalBytesWrittenUfsThroughput the total bytes written ufs throughput
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTotalBytesWrittenUfsThroughput(
      String TotalBytesWrittenUfsThroughput) {
    mTotalBytesWrittenUfsThroughput = TotalBytesWrittenUfsThroughput;
    return this;
  }

  /**
   * Sets ufs ops.
   *
   * @param UfsOps the ufs ops
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setUfsOps(Map<String, Map<String, Long>> UfsOps) {
    mUfsOps = UfsOps;
    return this;
  }

  /**
   * Sets ufs saved ops.
   *
   * @param ufsOpsSavedMap the ufs ops
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setUfsOpsSaved(Map<String, Map<String, Long>> ufsOpsSavedMap) {
    mUfsOpsSaved = ufsOpsSavedMap;
    return this;
  }

  /**
   * Sets ufs read size.
   *
   * @param UfsReadSize the ufs read size
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setUfsReadSize(Map<String, String> UfsReadSize) {
    mUfsReadSize = UfsReadSize;
    return this;
  }

  /**
   * Sets ufs write size.
   *
   * @param UfsWriteSize the ufs write size
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setUfsWriteSize(Map<String, String> UfsWriteSize) {
    mUfsWriteSize = UfsWriteSize;
    return this;
  }

  /**
   * Sets operation metrics.
   *
   * @param operationMetrics the operation metrics
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setOperationMetrics(Map<String, Metric> operationMetrics) {
    mOperationMetrics = operationMetrics;
    return this;
  }

  /**
   * @param timeSeries the time series metrics to set. The latest 20 data points will be set
   * @return the updated masterWebUIMetrics object
   */
  public MasterWebUIMetrics setTimeSeriesMetrics(List<TimeSeries> timeSeries) {
    mTimeSeriesMetrics = timeSeries;
    return this;
  }

  /**
   * @param journalDiskMetrics the disk metrics to set
   * @return the updated {@link MasterWebUIMetrics} object
   */
  public MasterWebUIMetrics setJournalDiskMetrics(List<JournalDiskInfo> journalDiskMetrics) {
    mJournalDiskMetrics = journalDiskMetrics;
    return this;
  }

  /**
   * @param lastCheckpointTime the last journal checkpoint time
   * @return the updated metrics object
   */
  public MasterWebUIMetrics setJournalLastCheckpointTime(String lastCheckpointTime) {
    mJournalLastCheckpointTime = lastCheckpointTime;
    return this;
  }

  /**
   * @param entriesSinceCheckpoint the last journal checkpoint time
   * @return the updated metrics object
   */
  public MasterWebUIMetrics setJournalEntriesSinceCheckpoint(long entriesSinceCheckpoint) {
    mJournalEntriesSinceCheckpoint = entriesSinceCheckpoint;
    return this;
  }

  /**
   * Sets rpc invocation metrics.
   *
   * @param rpcInvocationMetrics the rpc invocation metrics
   * @return the rpc invocation metrics
   */
  public MasterWebUIMetrics setRpcInvocationMetrics(Map<String, Counter> rpcInvocationMetrics) {
    mRpcInvocationMetrics = rpcInvocationMetrics;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("cacheHit", mCacheHit).add("cacheMiss", mCacheMiss)
        .add("masterCapacityFreePercentage", mMasterCapacityFreePercentage)
        .add("masterCapacityUsedPercentage", mMasterCapacityUsedPercentage)
        .add("masterUnderfsCapacityFreePercentage", mMasterUnderfsCapacityFreePercentage)
        .add("masterUnderfsCapacityUsedPercentage", mMasterUnderfsCapacityUsedPercentage)
        .add("totalBytesReadDomainSocket", mTotalBytesReadDomainSocket)
        .add("totalBytesReadDomainSocketThroughput", mTotalBytesReadDomainSocketThroughput)
        .add("totalBytesReadRemote", mTotalBytesReadRemote)
        .add("totalBytesReadRemoteThroughput", mTotalBytesReadRemoteThroughput)
        .add("totalBytesReadUfs", mTotalBytesReadUfs)
        .add("totalBytesReadUfsThroughput", mTotalBytesReadUfsThroughput)
        .add("totalBytesWrittenRemote", mTotalBytesWrittenRemote)
        .add("totalBytesWrittenRemoteThroughput", mTotalBytesWrittenRemoteThroughput)
        .add("totalBytesWrittenUfs", mTotalBytesWrittenUfs)
        .add("totalBytesWrittenUfsThroughput", mTotalBytesWrittenUfsThroughput)
        .add("ufsOps", mUfsOps).add("ufsReadSize", mUfsReadSize).add("ufsWriteSize", mUfsWriteSize)
        .add("timeSeriesMetrics", mTimeSeriesMetrics)
        .toString();
  }
}
