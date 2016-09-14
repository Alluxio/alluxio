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

package alluxio.worker;

import alluxio.metrics.MetricsSystem;
import alluxio.metrics.source.Source;
import alluxio.worker.block.BlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Worker source collects a worker's internal state.
 */
@NotThreadSafe
public class WorkerSource implements Source {
  private static final String WORKER_SOURCE_NAME = "worker";

  public static final String BLOCKS_READ_LOCAL = "BlocksReadLocal";
  public static final String BLOCKS_READ_REMOTE = "BlocksReadRemote";
  public static final String BLOCKS_WRITTEN_LOCAL = "BlocksWrittenLocal";
  public static final String BLOCKS_WRITTEN_REMOTE = "BlocksWrittenRemote";
  public static final String BYTES_READ_LOCAL = "BytesReadLocal";
  public static final String BYTES_READ_REMOTE = "BytesReadRemote";
  public static final String BYTES_READ_UFS = "BytesReadUfs";
  public static final String BYTES_WRITTEN_LOCAL = "BytesWrittenLocal";
  public static final String BYTES_WRITTEN_REMOTE = "BytesWrittenRemote";
  public static final String BYTES_WRITTEN_UFS = "BytesWrittenUfs";
  public static final String CAPACITY_TOTAL = "CapacityTotal";
  public static final String CAPACITY_USED = "CapacityUsed";
  public static final String CAPACITY_FREE = "CapacityFree";
  public static final String BLOCKS_CACHED = "BlocksCached";
  public static final String SEEKS_LOCAL = "SeeksLocal";
  public static final String SEEKS_REMOTE = "SeeksRemote";

  private final MetricRegistry mMetricRegistry = new MetricRegistry();

  // metrics from client
  private final Counter mBlocksReadLocal = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_READ_LOCAL));
  private final Counter mBlocksReadRemote = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_READ_REMOTE));
  private final Counter mBlocksWrittenLocal = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_WRITTEN_LOCAL));
  private final Counter mBlocksWrittenRemote = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_WRITTEN_REMOTE));
  private final Counter mBytesReadLocal = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_READ_LOCAL));
  private final Counter mBytesReadRemote = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_READ_REMOTE));
  private final Counter mBytesReadUfs = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_READ_UFS));
  private final Counter mBytesWrittenLocal = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_WRITTEN_LOCAL));
  private final Counter mBytesWrittenRemote = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_WRITTEN_REMOTE));
  private final Counter mBytesWrittenUfs = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_WRITTEN_UFS));
  private final Counter mSeeksLocal = mMetricRegistry.counter(MetricRegistry.name(SEEKS_LOCAL));
  private final Counter mSeeksRemote = mMetricRegistry.counter(MetricRegistry.name(SEEKS_REMOTE));

  /**
   * Constructs a new {@link WorkerSource}.
   */
  public WorkerSource() {}

  @Override
  public String getName() {
    return WORKER_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }

  /**
   * Increments the counter of blocks read locally.
   *
   * @param n the increment
   */
  public void incBlocksReadLocal(long n) {
    mBlocksReadLocal.inc(n);
  }

  /**
   * Increments the counter of blocks read remotely.
   *
   * @param n the increment
   */
  public void incBlocksReadRemote(long n) {
    mBlocksReadRemote.inc(n);
  }

  /**
   * Increments the counter of blocks written locally.
   *
   * @param n the increment
   */
  public void incBlocksWrittenLocal(long n) {
    mBlocksWrittenLocal.inc(n);
  }

  /**
   * Increments the counter of blocks written remotely.
   *
   * @param n the increment
   */
  public void incBlocksWrittenRemote(long n) {
    mBlocksWrittenRemote.inc(n);
  }

  /**
   * Increments the counter of bytes read locally.
   *
   * @param n the increment
   */
  public void incBytesReadLocal(long n) {
    mBytesReadLocal.inc(n);
  }

  /**
   * Increments the counter of bytes read remotely.
   *
   * @param n the increment
   */
  public void incBytesReadRemote(long n) {
    mBytesReadRemote.inc(n);
  }

  /**
   * Increments the counter of bytes read from UFS.
   *
   * @param n the increment
   */
  public void incBytesReadUfs(long n) {
    mBytesReadUfs.inc(n);
  }

  /**
   * Increments the counter of bytes written locally.
   *
   * @param n the increment
   */
  public void incBytesWrittenLocal(long n) {
    mBytesWrittenLocal.inc(n);
  }

  /**
   * Increments the counter of bytes written remotely.
   *
   * @param n the increment
   */
  public void incBytesWrittenRemote(long n) {
    mBytesWrittenRemote.inc(n);
  }

  /**
   * Increments the counter of bytes written to UFS.
   *
   * @param n the increment
   */
  public void incBytesWrittenUfs(long n) {
    mBytesWrittenUfs.inc(n);
  }

  /**
   * Increments SEEKS_LOCAL counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incSeeksLocal(long n) {
    mSeeksLocal.inc(n);
  }

  /**
   * Increments SEEKS_REMOTE counter by the amount specified.
   *
   * @param n amount to increment
   */
  public synchronized void incSeeksRemote(long n) {
    mSeeksRemote.inc(n);
  }
}
