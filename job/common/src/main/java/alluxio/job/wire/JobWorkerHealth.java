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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The job worker health information.
 */
public class JobWorkerHealth {

  private final long mWorkerId;
  private final ArrayList<Double> mLoadAverage;
  private final long mLastUpdated;

  private String mHostname;

  /**
   * Default constructor.
   * @param workerId the worker id
   * @param loadAverage the cpu load average
   */
  public JobWorkerHealth(long workerId, double[] loadAverage) {
    mWorkerId = workerId;
    mLoadAverage = Lists.newArrayList();
    for (double l : loadAverage) {
      mLoadAverage.add(l);
    }
    mLastUpdated = CommonUtils.getCurrentMs();
  }

  /**
   * Constructor from the grpc representation.
   * @param jobWorkerHealth grpc representation
   */
  public JobWorkerHealth(alluxio.grpc.JobWorkerHealth jobWorkerHealth) {
    mWorkerId = jobWorkerHealth.getWorkerId();
    mLoadAverage = Lists.newArrayList(jobWorkerHealth.getLoadAverageList());
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
   * @return the load average
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
