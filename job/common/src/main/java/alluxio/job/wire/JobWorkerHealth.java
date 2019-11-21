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

package alluxio.job.wire;

import alluxio.util.CommonUtils;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

/**
 * The job worker health information.
 */
public class JobWorkerHealth {

  private final long mWorkerId;
  private final List<Double> mLoadAverage;
  private final long mLastUpdated;
  private final String mHostname;

  /**
   * Default constructor.
   *
   * @param workerId the worker id
   * @param loadAverage output of CentralProcessor.getSystemLoadAverage on the worker
   */
  public JobWorkerHealth(long workerId, double[] loadAverage, String hostname) {
    mWorkerId = workerId;
    mLoadAverage = DoubleStream.of(loadAverage).boxed().collect(Collectors.toList());
    mLastUpdated = CommonUtils.getCurrentMs();
    mHostname = hostname;
  }

  /**
   * Constructor from the grpc representation.
   *
   * @param jobWorkerHealth grpc representation
   */
  public JobWorkerHealth(alluxio.grpc.JobWorkerHealth jobWorkerHealth) {
    mWorkerId = jobWorkerHealth.getWorkerId();
    mLoadAverage = jobWorkerHealth.getLoadAverageList();
    mLastUpdated = jobWorkerHealth.getLastUpdated();
    mHostname = jobWorkerHealth.getHostname();
  }

  /**
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * Returns system load average for 1, 5, and 15 minutes. System load average is the sum of
   * runnable entities.
   * See http://oshi.github.io/oshi/apidocs/oshi/hardware/CentralProcessor.html#getSystemLoadAverage
   *
   * @return the load average for 1, 5, and 15 minutes. negative values if not available
   */
  public List<Double> getLoadAverage() {
    return Collections.unmodifiableList(mLoadAverage);
  }

  /**
   * @return the worker hostname
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * Sets the worker hostname.
   * @param hostname worker hostname
   */
  public void setHostname(String hostname) {
    mHostname = hostname;
  }

  /**
   * @return proto representation of JobWorkerInfo
   */
  public alluxio.grpc.JobWorkerHealth toProto() {
    alluxio.grpc.JobWorkerHealth.Builder builder = alluxio.grpc.JobWorkerHealth.newBuilder()
        .setWorkerId(mWorkerId).addAllLoadAverage(mLoadAverage).setLastUpdated(mLastUpdated);

    if (mHostname != null) {
      builder.setHostname(mHostname);
    }

    return builder.build();
  }
}
