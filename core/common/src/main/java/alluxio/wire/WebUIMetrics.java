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
public final class WebUIMetrics implements Serializable {
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
   * Creates a new instance of {@link WebUIMetrics}.
   */
  public WebUIMetrics() {
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

  public WebUIMetrics setCacheHitLocal(String CacheHitLocal) {
    mCacheHitLocal = CacheHitLocal;
    return this;
  }

  public WebUIMetrics setCacheHitRemote(String CacheHitRemote) {
    mCacheHitRemote = CacheHitRemote;
    return this;
  }

  public WebUIMetrics setCacheMiss(String CacheMiss) {
    mCacheMiss = CacheMiss;
    return this;
  }

  public WebUIMetrics setMasterCapacityFreePercentage(int MasterCapacityFreePercentage) {
    mMasterCapacityFreePercentage = MasterCapacityFreePercentage;
    return this;
  }

  public WebUIMetrics setMasterCapacityUsedPercentage(int MasterCapacityUsedPercentage) {
    mMasterCapacityUsedPercentage = MasterCapacityUsedPercentage;
    return this;
  }

  public WebUIMetrics setMasterUnderfsCapacityFreePercentage(
      int MasterUnderfsCapacityFreePercentage) {
    mMasterUnderfsCapacityFreePercentage = MasterUnderfsCapacityFreePercentage;
    return this;
  }

  public WebUIMetrics setMasterUnderfsCapacityUsedPercentage(
      int MasterUnderfsCapacityUsedPercentage) {
    mMasterUnderfsCapacityUsedPercentage = MasterUnderfsCapacityUsedPercentage;
    return this;
  }

  public WebUIMetrics setTotalBytesReadLocal(String TotalBytesReadLocal) {
    mTotalBytesReadLocal = TotalBytesReadLocal;
    return this;
  }

  public WebUIMetrics setTotalBytesReadLocalThroughput(String TotalBytesReadLocalThroughput) {
    mTotalBytesReadLocalThroughput = TotalBytesReadLocalThroughput;
    return this;
  }

  public WebUIMetrics setTotalBytesReadRemote(String TotalBytesReadRemote) {
    mTotalBytesReadRemote = TotalBytesReadRemote;
    return this;
  }

  public WebUIMetrics setTotalBytesReadRemoteThroughput(String TotalBytesReadRemoteThroughput) {
    mTotalBytesReadRemoteThroughput = TotalBytesReadRemoteThroughput;
    return this;
  }

  public WebUIMetrics setTotalBytesReadUfs(String TotalBytesReadUfs) {
    mTotalBytesReadUfs = TotalBytesReadUfs;
    return this;
  }

  public WebUIMetrics setTotalBytesReadUfsThroughput(String TotalBytesReadUfsThroughput) {
    mTotalBytesReadUfsThroughput = TotalBytesReadUfsThroughput;
    return this;
  }

  public WebUIMetrics setTotalBytesWrittenAlluxio(String TotalBytesWrittenAlluxio) {
    mTotalBytesWrittenAlluxio = TotalBytesWrittenAlluxio;
    return this;
  }

  public WebUIMetrics setTotalBytesWrittenAlluxioThroughput(
      String TotalBytesWrittenAlluxioThroughput) {
    mTotalBytesWrittenAlluxioThroughput = TotalBytesWrittenAlluxioThroughput;
    return this;
  }

  public WebUIMetrics setTotalBytesWrittenUfs(String TotalBytesWrittenUfs) {
    mTotalBytesWrittenUfs = TotalBytesWrittenUfs;
    return this;
  }

  public WebUIMetrics setTotalBytesWrittenUfsThroughput(String TotalBytesWrittenUfsThroughput) {
    mTotalBytesWrittenUfsThroughput = TotalBytesWrittenUfsThroughput;
    return this;
  }

  public WebUIMetrics setUfsOps(Map<String, Map<String, Long>> UfsOps) {
    mUfsOps = UfsOps;
    return this;
  }

  public WebUIMetrics setUfsReadSize(Map<String, String> UfsReadSize) {
    mUfsReadSize = UfsReadSize;
    return this;
  }

  public WebUIMetrics setUfsWriteSize(Map<String, String> UfsWriteSize) {
    mUfsWriteSize = UfsWriteSize;
    return this;
  }

  public WebUIMetrics setOperationMetrics(Map<String, Metric> operationMetrics) {
    mOperationMetrics = operationMetrics;
    return this;
  }

  public WebUIMetrics setRpcInvocationMetrics(Map<String, Counter> rpcInvocationMetrics) {
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
