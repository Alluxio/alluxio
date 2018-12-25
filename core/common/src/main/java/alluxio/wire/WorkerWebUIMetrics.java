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
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIMetrics implements Serializable {
  private long mWorkerCapacityUsedPercentage;
  private long mWorkerCapacityFreePercentage;
  private Map<String, Metric> mOperationMetrics;
  private Map<String, Counter> mRpcInvocationMetrics;

  /**
   * Creates a new instance of {@link WorkerWebUIMetrics}.
   */
  public WorkerWebUIMetrics() {
  }

  public long getWorkerCapacityUsedPercentage() {
    return mWorkerCapacityUsedPercentage;
  }

  public long getWorkerCapacityFreePercentage() {
    return mWorkerCapacityFreePercentage;
  }

  public Map<String, Metric> getOperationMetrics() {
    return mOperationMetrics;
  }

  public Map<String, Counter> getRpcInvocationMetrics() {
    return mRpcInvocationMetrics;
  }

  public WorkerWebUIMetrics setWorkerCapacityUsedPercentage(long WorkerCapacityUsedPercentage) {
    mWorkerCapacityUsedPercentage = WorkerCapacityUsedPercentage;
    return this;
  }

  public WorkerWebUIMetrics setWorkerCapacityFreePercentage(long WorkerCapacityFreePercentage) {
    mWorkerCapacityFreePercentage = WorkerCapacityFreePercentage;
    return this;
  }

  public WorkerWebUIMetrics setOperationMetrics(Map<String, Metric> OperationMetrics) {
    mOperationMetrics = OperationMetrics;
    return this;
  }

  public WorkerWebUIMetrics setRpcInvocationMetrics(Map<String, Counter> RpcInvocationMetrics) {
    mRpcInvocationMetrics = RpcInvocationMetrics;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("workerCapacityUsedPercentage", mWorkerCapacityUsedPercentage)
        .add("workerCapacityFreePercentage", mWorkerCapacityFreePercentage)
        .add("operationMetrics", mOperationMetrics)
        .add("rpcInvocationMetrics", mRpcInvocationMetrics).toString();
  }
}
