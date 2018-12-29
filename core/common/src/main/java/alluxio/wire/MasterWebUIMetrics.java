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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI metrics information.
 */
@NotThreadSafe
public final class MasterWebUIMetrics implements Serializable {
  private static final long serialVersionUID = 6589358568781503724L;

  private int mMasterCapacityFreePercentage;
  private int mMasterCapacityUsedPercentage;
  private int mMasterUnderfsCapacityFreePercentage;
  private int mMasterUnderfsCapacityUsedPercentage;
  private Map<String, Counter> mRpcInvocationMetrics;
  private Map<String, Map<String, Long>> mUfsOps;
  private Map<String, Metric> mOperationMetrics;
  private Map<String, String> mUfsReadSize;
  private Map<String, String> mUfsWriteSize;
  private String mCacheHitLocal;
  private String mCacheHitRemote;
  private String mCacheMiss;
  private String mTotalBytesReadLocal;
  private String mTotalBytesReadLocalThroughput;
  private String mTotalBytesReadRemote;
  private String mTotalBytesReadRemoteThroughput;
  private String mTotalBytesReadUfs;
  private String mTotalBytesReadUfsThroughput;
  private String mTotalBytesWrittenAlluxio;
  private String mTotalBytesWrittenAlluxioThroughput;
  private String mTotalBytesWrittenUfs;
  private String mTotalBytesWrittenUfsThroughput;

  /**
   * Creates a new instance of {@link MasterWebUIMetrics}.
   */
  public MasterWebUIMetrics() {
  }

  /**
   * Gets cache hit local.
   *
   * @return the cache hit local
   */
  public String getCacheHitLocal() {
    return mCacheHitLocal;
  }

  /**
   * Gets cache hit remote.
   *
   * @return the cache hit remote
   */
  public String getCacheHitRemote() {
    return mCacheHitRemote;
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
   * Gets total bytes read local.
   *
   * @return the total bytes read local
   */
  public String getTotalBytesReadLocal() {
    return mTotalBytesReadLocal;
  }

  /**
   * Gets total bytes read local throughput.
   *
   * @return the total bytes read local throughput
   */
  public String getTotalBytesReadLocalThroughput() {
    return mTotalBytesReadLocalThroughput;
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
   * Gets total bytes written alluxio.
   *
   * @return the total bytes written alluxio
   */
  public String getTotalBytesWrittenAlluxio() {
    return mTotalBytesWrittenAlluxio;
  }

  /**
   * Gets total bytes written alluxio throughput.
   *
   * @return the total bytes written alluxio throughput
   */
  public String getTotalBytesWrittenAlluxioThroughput() {
    return mTotalBytesWrittenAlluxioThroughput;
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
   * Sets cache hit local.
   *
   * @param CacheHitLocal the cache hit local
   * @return the cache hit local
   */
  public MasterWebUIMetrics setCacheHitLocal(String CacheHitLocal) {
    mCacheHitLocal = CacheHitLocal;
    return this;
  }

  /**
   * Sets cache hit remote.
   *
   * @param CacheHitRemote the cache hit remote
   * @return the cache hit remote
   */
  public MasterWebUIMetrics setCacheHitRemote(String CacheHitRemote) {
    mCacheHitRemote = CacheHitRemote;
    return this;
  }

  /**
   * Sets cache miss.
   *
   * @param CacheMiss the cache miss
   * @return the cache miss
   */
  public MasterWebUIMetrics setCacheMiss(String CacheMiss) {
    mCacheMiss = CacheMiss;
    return this;
  }

  /**
   * Sets master capacity free percentage.
   *
   * @param MasterCapacityFreePercentage the master capacity free percentage
   * @return the master capacity free percentage
   */
  public MasterWebUIMetrics setMasterCapacityFreePercentage(int MasterCapacityFreePercentage) {
    mMasterCapacityFreePercentage = MasterCapacityFreePercentage;
    return this;
  }

  /**
   * Sets master capacity used percentage.
   *
   * @param MasterCapacityUsedPercentage the master capacity used percentage
   * @return the master capacity used percentage
   */
  public MasterWebUIMetrics setMasterCapacityUsedPercentage(int MasterCapacityUsedPercentage) {
    mMasterCapacityUsedPercentage = MasterCapacityUsedPercentage;
    return this;
  }

  /**
   * Sets master underfs capacity free percentage.
   *
   * @param MasterUnderfsCapacityFreePercentage the master underfs capacity free percentage
   * @return the master underfs capacity free percentage
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
   * @return the master underfs capacity used percentage
   */
  public MasterWebUIMetrics setMasterUnderfsCapacityUsedPercentage(
      int MasterUnderfsCapacityUsedPercentage) {
    mMasterUnderfsCapacityUsedPercentage = MasterUnderfsCapacityUsedPercentage;
    return this;
  }

  /**
   * Sets total bytes read local.
   *
   * @param TotalBytesReadLocal the total bytes read local
   * @return the total bytes read local
   */
  public MasterWebUIMetrics setTotalBytesReadLocal(String TotalBytesReadLocal) {
    mTotalBytesReadLocal = TotalBytesReadLocal;
    return this;
  }

  /**
   * Sets total bytes read local throughput.
   *
   * @param TotalBytesReadLocalThroughput the total bytes read local throughput
   * @return the total bytes read local throughput
   */
  public MasterWebUIMetrics setTotalBytesReadLocalThroughput(String TotalBytesReadLocalThroughput) {
    mTotalBytesReadLocalThroughput = TotalBytesReadLocalThroughput;
    return this;
  }

  /**
   * Sets total bytes read remote.
   *
   * @param TotalBytesReadRemote the total bytes read remote
   * @return the total bytes read remote
   */
  public MasterWebUIMetrics setTotalBytesReadRemote(String TotalBytesReadRemote) {
    mTotalBytesReadRemote = TotalBytesReadRemote;
    return this;
  }

  /**
   * Sets total bytes read remote throughput.
   *
   * @param TotalBytesReadRemoteThroughput the total bytes read remote throughput
   * @return the total bytes read remote throughput
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
   * @return the total bytes read ufs
   */
  public MasterWebUIMetrics setTotalBytesReadUfs(String TotalBytesReadUfs) {
    mTotalBytesReadUfs = TotalBytesReadUfs;
    return this;
  }

  /**
   * Sets total bytes read ufs throughput.
   *
   * @param TotalBytesReadUfsThroughput the total bytes read ufs throughput
   * @return the total bytes read ufs throughput
   */
  public MasterWebUIMetrics setTotalBytesReadUfsThroughput(String TotalBytesReadUfsThroughput) {
    mTotalBytesReadUfsThroughput = TotalBytesReadUfsThroughput;
    return this;
  }

  /**
   * Sets total bytes written alluxio.
   *
   * @param TotalBytesWrittenAlluxio the total bytes written alluxio
   * @return the total bytes written alluxio
   */
  public MasterWebUIMetrics setTotalBytesWrittenAlluxio(String TotalBytesWrittenAlluxio) {
    mTotalBytesWrittenAlluxio = TotalBytesWrittenAlluxio;
    return this;
  }

  /**
   * Sets total bytes written alluxio throughput.
   *
   * @param TotalBytesWrittenAlluxioThroughput the total bytes written alluxio throughput
   * @return the total bytes written alluxio throughput
   */
  public MasterWebUIMetrics setTotalBytesWrittenAlluxioThroughput(
      String TotalBytesWrittenAlluxioThroughput) {
    mTotalBytesWrittenAlluxioThroughput = TotalBytesWrittenAlluxioThroughput;
    return this;
  }

  /**
   * Sets total bytes written ufs.
   *
   * @param TotalBytesWrittenUfs the total bytes written ufs
   * @return the total bytes written ufs
   */
  public MasterWebUIMetrics setTotalBytesWrittenUfs(String TotalBytesWrittenUfs) {
    mTotalBytesWrittenUfs = TotalBytesWrittenUfs;
    return this;
  }

  /**
   * Sets total bytes written ufs throughput.
   *
   * @param TotalBytesWrittenUfsThroughput the total bytes written ufs throughput
   * @return the total bytes written ufs throughput
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
   * @return the ufs ops
   */
  public MasterWebUIMetrics setUfsOps(Map<String, Map<String, Long>> UfsOps) {
    mUfsOps = UfsOps;
    return this;
  }

  /**
   * Sets ufs read size.
   *
   * @param UfsReadSize the ufs read size
   * @return the ufs read size
   */
  public MasterWebUIMetrics setUfsReadSize(Map<String, String> UfsReadSize) {
    mUfsReadSize = UfsReadSize;
    return this;
  }

  /**
   * Sets ufs write size.
   *
   * @param UfsWriteSize the ufs write size
   * @return the ufs write size
   */
  public MasterWebUIMetrics setUfsWriteSize(Map<String, String> UfsWriteSize) {
    mUfsWriteSize = UfsWriteSize;
    return this;
  }

  /**
   * Sets operation metrics.
   *
   * @param operationMetrics the operation metrics
   * @return the operation metrics
   */
  public MasterWebUIMetrics setOperationMetrics(Map<String, Metric> operationMetrics) {
    mOperationMetrics = operationMetrics;
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
    return Objects.toStringHelper(this).add("cacheHitLocal", mCacheHitLocal)
        .add("cacheHitRemote", mCacheHitRemote).add("cacheMiss", mCacheMiss)
        .add("masterCapacityFreePercentage", mMasterCapacityFreePercentage)
        .add("masterCapacityUsedPercentage", mMasterCapacityUsedPercentage)
        .add("masterUnderfsCapacityFreePercentage", mMasterUnderfsCapacityFreePercentage)
        .add("masterUnderfsCapacityUsedPercentage", mMasterUnderfsCapacityUsedPercentage)
        .add("totalBytesReadLocal", mTotalBytesReadLocal)
        .add("totalBytesReadLocalThroughput", mTotalBytesReadLocalThroughput)
        .add("totalBytesReadRemote", mTotalBytesReadRemote)
        .add("totalBytesReadRemoteThroughput", mTotalBytesReadRemoteThroughput)
        .add("totalBytesReadUfs", mTotalBytesReadUfs)
        .add("totalBytesReadUfsThroughput", mTotalBytesReadUfsThroughput)
        .add("totalBytesWrittenAlluxio", mTotalBytesWrittenAlluxio)
        .add("totalBytesWrittenAlluxioThroughput", mTotalBytesWrittenAlluxioThroughput)
        .add("totalBytesWrittenUfs", mTotalBytesWrittenUfs)
        .add("totalBytesWrittenUfsThroughput", mTotalBytesWrittenUfsThroughput)
        .add("ufsOps", mUfsOps).add("ufsReadSize", mUfsReadSize).add("ufsWriteSize", mUfsWriteSize)
        .toString();
  }
}
