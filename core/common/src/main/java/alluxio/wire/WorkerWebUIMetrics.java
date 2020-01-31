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

import com.codahale.metrics.Metric;
import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIMetrics implements Serializable {
  private static final long serialVersionUID = 2592682454201543777L;

  private long mWorkerCapacityFreePercentage;
  private long mWorkerCapacityUsedPercentage;
  private Map<String, Metric> mOperationMetrics;

  /**
   * Creates a new instance of {@link WorkerWebUIMetrics}.
   */
  public WorkerWebUIMetrics() {
  }

  /**
   * Gets worker capacity used percentage.
   *
   * @return the worker capacity used percentage
   */
  public long getWorkerCapacityUsedPercentage() {
    return mWorkerCapacityUsedPercentage;
  }

  /**
   * Gets worker capacity free percentage.
   *
   * @return the worker capacity free percentage
   */
  public long getWorkerCapacityFreePercentage() {
    return mWorkerCapacityFreePercentage;
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
   * Sets worker capacity used percentage.
   *
   * @param WorkerCapacityUsedPercentage the worker capacity used percentage
   * @return the worker capacity used percentage
   */
  public WorkerWebUIMetrics setWorkerCapacityUsedPercentage(long WorkerCapacityUsedPercentage) {
    mWorkerCapacityUsedPercentage = WorkerCapacityUsedPercentage;
    return this;
  }

  /**
   * Sets worker capacity free percentage.
   *
   * @param WorkerCapacityFreePercentage the worker capacity free percentage
   * @return the worker capacity free percentage
   */
  public WorkerWebUIMetrics setWorkerCapacityFreePercentage(long WorkerCapacityFreePercentage) {
    mWorkerCapacityFreePercentage = WorkerCapacityFreePercentage;
    return this;
  }

  /**
   * Sets operation metrics.
   *
   * @param OperationMetrics the operation metrics
   * @return the operation metrics
   */
  public WorkerWebUIMetrics setOperationMetrics(Map<String, Metric> OperationMetrics) {
    mOperationMetrics = OperationMetrics;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerCapacityUsedPercentage", mWorkerCapacityUsedPercentage)
        .add("workerCapacityFreePercentage", mWorkerCapacityFreePercentage)
        .add("operationMetrics", mOperationMetrics).toString();
  }
}
