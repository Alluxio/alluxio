/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * ClientMetrics is used to pass client metrics from client to worker by session heartbeat.
 */
@ThreadSafe
public final class ClientMetrics {
  private List<Long> mMetrics;

  /**
   * Constructs new client metrics.
   */
  public ClientMetrics() {
    mMetrics = createDefaultMetrics();
  }

  private List<Long> createDefaultMetrics() {
    List<Long> defaultMetrics =
        new ArrayList<Long>(Collections.nCopies(Constants.CLIENT_METRICS_SIZE, 0L));
    defaultMetrics.set(Constants.CLIENT_METRICS_VERSION_INDEX, Constants.CLIENT_METRICS_VERSION);
    return defaultMetrics;
  }

  /**
   * Returns current metrics as heartbeat data and reset metrics to default.
   *
   * @return current metrics (list of longs)
   */
  public synchronized List<Long> getHeartbeatData() {
    List<Long> ret = mMetrics;
    mMetrics = createDefaultMetrics();
    return ret;
  }

  /**
   * Increments BLOCKS_READ_LOCAL counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBlocksReadLocal(long n) {
    mMetrics.set(Constants.BLOCKS_READ_LOCAL_INDEX,
        mMetrics.get(Constants.BLOCKS_READ_LOCAL_INDEX) + n);
  }

  /**
   * Increments BLOCKS_READ_REMOTE counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBlocksReadRemote(long n) {
    mMetrics.set(Constants.BLOCKS_READ_REMOTE_INDEX,
        mMetrics.get(Constants.BLOCKS_READ_REMOTE_INDEX) + n);
  }

  /**
   * Increments BLOCKS_WRITTEN_LOCAL counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBlocksWrittenLocal(long n) {
    mMetrics.set(Constants.BLOCKS_WRITTEN_LOCAL_INDEX,
        mMetrics.get(Constants.BLOCKS_WRITTEN_LOCAL_INDEX) + n);
  }

  /**
   * Increments BLOCKS_WRITTEN_REMOTE counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBlocksWrittenRemote(long n) {
    mMetrics.set(Constants.BLOCKS_WRITTEN_REMOTE_INDEX,
        mMetrics.get(Constants.BLOCKS_WRITTEN_REMOTE_INDEX) + n);
  }

  /**
   * Increments BYTES_READ_LOCAL counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBytesReadLocal(long n) {
    mMetrics.set(Constants.BYTES_READ_LOCAL_INDEX,
        mMetrics.get(Constants.BYTES_READ_LOCAL_INDEX) + n);
  }

  /**
   * Increments BYTES_READ_REMOTE counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBytesReadRemote(long n) {
    mMetrics.set(Constants.BYTES_READ_REMOTE_INDEX,
        mMetrics.get(Constants.BYTES_READ_REMOTE_INDEX) + n);
  }

  /**
   * Increments BYTES_READ_UFS counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBytesReadUfs(long n) {
    mMetrics.set(Constants.BYTES_READ_UFS_INDEX, mMetrics.get(Constants.BYTES_READ_UFS_INDEX) + n);
  }

  /**
   * Increments BYTES_WRITTEN_LOCAL counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBytesWrittenLocal(long n) {
    mMetrics.set(Constants.BYTES_WRITTEN_LOCAL_INDEX,
        mMetrics.get(Constants.BYTES_WRITTEN_LOCAL_INDEX) + n);
  }

  /**
   * Increments BYTES_WRITTEN_REMOTE counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBytesWrittenRemote(long n) {
    mMetrics.set(Constants.BYTES_WRITTEN_REMOTE_INDEX,
        mMetrics.get(Constants.BYTES_WRITTEN_REMOTE_INDEX) + n);
  }

  /**
   * Increments BYTES_WRITTEN_UFS counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incBytesWrittenUfs(long n) {
    mMetrics.set(Constants.BYTES_WRITTEN_UFS_INDEX,
        mMetrics.get(Constants.BYTES_WRITTEN_UFS_INDEX) + n);
  }
}
