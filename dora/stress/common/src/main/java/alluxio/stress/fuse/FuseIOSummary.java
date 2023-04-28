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

package alluxio.stress.fuse;

import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.common.GeneralBenchSummary;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The summary for Fuse IO stress bench.
 */
public class FuseIOSummary extends GeneralBenchSummary<FuseIOTaskResult> {
  private FuseIOParameters mParameters;
  private BaseParameters mBaseParameters;
  private long mRecordStartMs;
  private long mEndMs;
  private long mIOBytes;
  private float mIOMBps;

  /**
   * Default constructor required for json deserialization.
   */
  public FuseIOSummary() {
    this(null, null, new HashMap<>(), 0, 0, 0, 0);
  }

  /**
   * Creates an instance.
   *
   * @param parameters the parameters for the Fuse IO stress bench
   * @param baseParameters the base parameters for the Fuse IO stress bench
   * @param nodes the unique ids of all job workers
   * @param recordStartMs the timestamp starting counting bytes
   * @param endMs the timestamp that the test ends
   * @param ioBytes total number of bytes processed by workers
   * @param ioMBps aggregated throughput data
   */
  public FuseIOSummary(FuseIOParameters parameters, BaseParameters baseParameters,
      Map<String, FuseIOTaskResult> nodes, long recordStartMs, long endMs,
      long ioBytes, float ioMBps) {
    mNodeResults = nodes;
    mParameters = parameters;
    mBaseParameters = baseParameters;
    mRecordStartMs = recordStartMs;
    mEndMs = endMs;
    mIOBytes = ioBytes;
    mThroughput = ioMBps;
    mIOMBps = mThroughput;
  }

  @Override
  @Nullable
  public GraphGenerator graphGenerator() {
    return null;
  }

  /**
   * @return Fuse IO stress bench parameters
   */
  public FuseIOParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters Fuse IO stress bench parameters
   */
  public void setParameters(FuseIOParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the base parameters
   */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the base parameters
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the timestamp starting counting bytes (in ms)
   */
  public long getRecordStartMs() {
    return mRecordStartMs;
  }

  /**
   * @param recordStartMs the timestamp starting counting bytes (in ms)
   */
  public void setRecordStartMs(long recordStartMs) {
    mRecordStartMs = recordStartMs;
  }

  /**
   * @return the timestamp that test ends (in ms)
   */
  public long getEndMs() {
    return mEndMs;
  }

  /**
   * @param endMs the timestamp that test ends (in ms)
   */
  public void setEndMs(long endMs) {
    mEndMs = endMs;
  }

  /**
   * @return total number of bytes processed during test time
   */
  public long getIOBytes() {
    return mIOBytes;
  }

  /**
   * @param ioBytes total number of bytes processed during test time
   */
  public void setIOBytes(long ioBytes) {
    mIOBytes = ioBytes;
  }

  /**
   * @return overall throughput (in MB / s)
   */
  public float getIOMBps() {
    return mIOMBps;
  }

  /**
   * @param ioMBps overall throughput (in MB / s)
   */
  public void setIOMBps(float ioMBps) {
    mIOMBps = ioMBps;
  }
}
