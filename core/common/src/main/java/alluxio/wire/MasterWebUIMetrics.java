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
  private String mCacheHitLocal;
  private String mCacheHitRemote;
  private String mCacheMiss;
  private int mMasterCapacityFreePercentage;
  private int mMasterCapacityUsedPercentage;
  private int mMasterUnderfsCapacityFreePercentage;
  private int mMasterUnderfsCapacityUsedPercentage;
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
  private Map<String, Map<String, Long>> mUfsOps;
  private Map<String, String> mUfsReadSize;
  private Map<String, String> mUfsWriteSize;
  private Map<String, Metric> mOperationMetrics;
  private Map<String, Counter> mRpcInvocationMetrics;

  /**
   * Creates a new instance of {@link MasterWebUIMetrics}.
   */
  public MasterWebUIMetrics() {
  }

  public String getCacheHitLocal() {
    return mCacheHitLocal;
  }

  public String getCacheHitRemote() {
    return mCacheHitRemote;
  }

  public String getCacheMiss() {
    return mCacheMiss;
  }

  public int getMasterCapacityFreePercentage() {
    return mMasterCapacityFreePercentage;
  }

  public int getMasterCapacityUsedPercentage() {
    return mMasterCapacityUsedPercentage;
  }

  public int getMasterUnderfsCapacityFreePercentage() {
    return mMasterUnderfsCapacityFreePercentage;
  }

  public int getMasterUnderfsCapacityUsedPercentage() {
    return mMasterUnderfsCapacityUsedPercentage;
  }

  public String getTotalBytesReadLocal() {
    return mTotalBytesReadLocal;
  }

  public String getTotalBytesReadLocalThroughput() {
    return mTotalBytesReadLocalThroughput;
  }

  public String getTotalBytesReadRemote() {
    return mTotalBytesReadRemote;
  }

  public String getTotalBytesReadRemoteThroughput() {
    return mTotalBytesReadRemoteThroughput;
  }

  public String getTotalBytesReadUfs() {
    return mTotalBytesReadUfs;
  }

  public String getTotalBytesReadUfsThroughput() {
    return mTotalBytesReadUfsThroughput;
  }

  public String getTotalBytesWrittenAlluxio() {
    return mTotalBytesWrittenAlluxio;
  }

  public String getTotalBytesWrittenAlluxioThroughput() {
    return mTotalBytesWrittenAlluxioThroughput;
  }

  public String getTotalBytesWrittenUfs() {
    return mTotalBytesWrittenUfs;
  }

  public String getTotalBytesWrittenUfsThroughput() {
    return mTotalBytesWrittenUfsThroughput;
  }

  public Map<String, Map<String, Long>> getUfsOps() {
    return mUfsOps;
  }

  public Map<String, String> getUfsReadSize() {
    return mUfsReadSize;
  }

  public Map<String, String> getUfsWriteSize() {
    return mUfsWriteSize;
  }

  public Map<String, Metric> getOperationMetrics() {
    return mOperationMetrics;
  }

  public Map<String, Counter> getRpcInvocationMetrics() {
    return mRpcInvocationMetrics;
  }

  public MasterWebUIMetrics setCacheHitLocal(String CacheHitLocal) {
    mCacheHitLocal = CacheHitLocal;
    return this;
  }

  public MasterWebUIMetrics setCacheHitRemote(String CacheHitRemote) {
    mCacheHitRemote = CacheHitRemote;
    return this;
  }

  public MasterWebUIMetrics setCacheMiss(String CacheMiss) {
    mCacheMiss = CacheMiss;
    return this;
  }

  public MasterWebUIMetrics setMasterCapacityFreePercentage(int MasterCapacityFreePercentage) {
    mMasterCapacityFreePercentage = MasterCapacityFreePercentage;
    return this;
  }

  public MasterWebUIMetrics setMasterCapacityUsedPercentage(int MasterCapacityUsedPercentage) {
    mMasterCapacityUsedPercentage = MasterCapacityUsedPercentage;
    return this;
  }

  public MasterWebUIMetrics setMasterUnderfsCapacityFreePercentage(
      int MasterUnderfsCapacityFreePercentage) {
    mMasterUnderfsCapacityFreePercentage = MasterUnderfsCapacityFreePercentage;
    return this;
  }

  public MasterWebUIMetrics setMasterUnderfsCapacityUsedPercentage(
      int MasterUnderfsCapacityUsedPercentage) {
    mMasterUnderfsCapacityUsedPercentage = MasterUnderfsCapacityUsedPercentage;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesReadLocal(String TotalBytesReadLocal) {
    mTotalBytesReadLocal = TotalBytesReadLocal;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesReadLocalThroughput(String TotalBytesReadLocalThroughput) {
    mTotalBytesReadLocalThroughput = TotalBytesReadLocalThroughput;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesReadRemote(String TotalBytesReadRemote) {
    mTotalBytesReadRemote = TotalBytesReadRemote;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesReadRemoteThroughput(String TotalBytesReadRemoteThroughput) {
    mTotalBytesReadRemoteThroughput = TotalBytesReadRemoteThroughput;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesReadUfs(String TotalBytesReadUfs) {
    mTotalBytesReadUfs = TotalBytesReadUfs;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesReadUfsThroughput(String TotalBytesReadUfsThroughput) {
    mTotalBytesReadUfsThroughput = TotalBytesReadUfsThroughput;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesWrittenAlluxio(String TotalBytesWrittenAlluxio) {
    mTotalBytesWrittenAlluxio = TotalBytesWrittenAlluxio;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesWrittenAlluxioThroughput(
      String TotalBytesWrittenAlluxioThroughput) {
    mTotalBytesWrittenAlluxioThroughput = TotalBytesWrittenAlluxioThroughput;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesWrittenUfs(String TotalBytesWrittenUfs) {
    mTotalBytesWrittenUfs = TotalBytesWrittenUfs;
    return this;
  }

  public MasterWebUIMetrics setTotalBytesWrittenUfsThroughput(String TotalBytesWrittenUfsThroughput) {
    mTotalBytesWrittenUfsThroughput = TotalBytesWrittenUfsThroughput;
    return this;
  }

  public MasterWebUIMetrics setUfsOps(Map<String, Map<String, Long>> UfsOps) {
    mUfsOps = UfsOps;
    return this;
  }

  public MasterWebUIMetrics setUfsReadSize(Map<String, String> UfsReadSize) {
    mUfsReadSize = UfsReadSize;
    return this;
  }

  public MasterWebUIMetrics setUfsWriteSize(Map<String, String> UfsWriteSize) {
    mUfsWriteSize = UfsWriteSize;
    return this;
  }

  public MasterWebUIMetrics setOperationMetrics(Map<String, Metric> operationMetrics) {
    mOperationMetrics = operationMetrics;
    return this;
  }

  public MasterWebUIMetrics setRpcInvocationMetrics(Map<String, Counter> rpcInvocationMetrics) {
    mRpcInvocationMetrics = rpcInvocationMetrics;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mCacheHitLocal", mCacheHitLocal)
        .add("mCacheHitRemote", mCacheHitRemote).add("mCacheMiss", mCacheMiss)
        .add("mMasterCapacityFreePercentage", mMasterCapacityFreePercentage)
        .add("mMasterCapacityUsedPercentage", mMasterCapacityUsedPercentage)
        .add("mMasterUnderfsCapacityFreePercentage", mMasterUnderfsCapacityFreePercentage)
        .add("mMasterUnderfsCapacityUsedPercentage", mMasterUnderfsCapacityUsedPercentage)
        .add("mTotalBytesReadLocal", mTotalBytesReadLocal)
        .add("mTotalBytesReadLocalThroughput", mTotalBytesReadLocalThroughput)
        .add("mTotalBytesReadRemote", mTotalBytesReadRemote)
        .add("mTotalBytesReadRemoteThroughput", mTotalBytesReadRemoteThroughput)
        .add("mTotalBytesReadUfs", mTotalBytesReadUfs)
        .add("mTotalBytesReadUfsThroughput", mTotalBytesReadUfsThroughput)
        .add("mTotalBytesWrittenAlluxio", mTotalBytesWrittenAlluxio)
        .add("mTotalBytesWrittenAlluxioThroughput", mTotalBytesWrittenAlluxioThroughput)
        .add("mTotalBytesWrittenUfs", mTotalBytesWrittenUfs)
        .add("mTotalBytesWrittenUfsThroughput", mTotalBytesWrittenUfsThroughput)
        .add("mUfsOps", mUfsOps).add("mUfsReadSize", mUfsReadSize)
        .add("mUfsWriteSize", mUfsWriteSize).toString();
  }
}
