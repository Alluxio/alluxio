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

import alluxio.RuntimeConstants;
import alluxio.grpc.BuildVersion;
import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;

/**
 * The job worker health information.
 */
public class JobWorkerHealth {
  private final long mWorkerId;
  private BuildVersion mVersion = RuntimeConstants.UNKNOWN_VERSION_INFO;
  private final List<Double> mLoadAverage;
  private final int mUnfinishedTasks;
  private final long mLastUpdated;
  private final int mTaskPoolSize;
  private final int mNumActiveTasks;
  private final String mHostname;

  /**
   * Default constructor.
   *
   * @param workerId the worker id
   * @param loadAverage output of CentralProcessor.getSystemLoadAverage on the worker
   * @param taskPoolSize task pool size
   * @param numActiveTasks number of active tasks in the worker
   * @param unfinishedTasks number of unfinished tasks that the worker has
   * @param hostname hostname of the worker
   */
  public JobWorkerHealth(long workerId, List<Double> loadAverage, int taskPoolSize,
      int numActiveTasks, int unfinishedTasks, String hostname) {
    this(workerId, loadAverage, taskPoolSize, numActiveTasks, unfinishedTasks, hostname,
        RuntimeConstants.CURRENT_VERSION_INFO);
  }

  /**
   * Default constructor.
   *
   * @param workerId the worker id
   * @param loadAverage output of CentralProcessor.getSystemLoadAverage on the worker
   * @param taskPoolSize task pool size
   * @param numActiveTasks number of active tasks in the worker
   * @param unfinishedTasks number of unfinished tasks that the worker has
   * @param hostname hostname of the worker
   * @param version the worker's version info
   */
  public JobWorkerHealth(long workerId, List<Double> loadAverage, int taskPoolSize,
                         int numActiveTasks, int unfinishedTasks, String hostname,
                         BuildVersion version) {
    mWorkerId = workerId;
    mLoadAverage = loadAverage;
    mUnfinishedTasks = unfinishedTasks;
    mLastUpdated = CommonUtils.getCurrentMs();
    mTaskPoolSize = taskPoolSize;
    mNumActiveTasks = numActiveTasks;
    mHostname = hostname;
    mVersion = version;
  }

  /**
   * Constructor from the grpc representation.
   *
   * @param jobWorkerHealth grpc representation
   */
  public JobWorkerHealth(alluxio.grpc.JobWorkerHealth jobWorkerHealth) {
    mWorkerId = jobWorkerHealth.getWorkerId();
    mLoadAverage = jobWorkerHealth.getLoadAverageList();
    mUnfinishedTasks = jobWorkerHealth.getUnfinishedTasks();
    mLastUpdated = jobWorkerHealth.getLastUpdated();
    mTaskPoolSize = jobWorkerHealth.getTaskPoolSize();
    mNumActiveTasks = jobWorkerHealth.getNumActiveTasks();
    mHostname = jobWorkerHealth.getHostname();
    if (jobWorkerHealth.hasVersion()) {
      mVersion = jobWorkerHealth.getVersion();
    }
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
   * @return task pool size
   */
  public int getTaskPoolSize() {
    return mTaskPoolSize;
  }

  /**
   * @return number of active tasks
   */
  public int getNumActiveTasks() {
    return mNumActiveTasks;
  }

  /**
   * @return the number of unfinished tasks
   */
  public int getUnfinishedTasks() {
    return mUnfinishedTasks;
  }

  /**
   * @return the worker hostname
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * @return the worker version info
   */
  public BuildVersion getVersion() {
    return mVersion;
  }

  /**
   * @return proto representation of JobWorkerInfo
   */
  public alluxio.grpc.JobWorkerHealth toProto() {
    alluxio.grpc.JobWorkerHealth.Builder builder = alluxio.grpc.JobWorkerHealth.newBuilder()
        .setWorkerId(mWorkerId).addAllLoadAverage(mLoadAverage).setUnfinishedTasks(mUnfinishedTasks)
        .setTaskPoolSize(mTaskPoolSize).setNumActiveTasks(mNumActiveTasks)
        .setLastUpdated(mLastUpdated).setHostname(mHostname).setVersion(mVersion);

    return builder.build();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerId, mLoadAverage, mLastUpdated, mHostname, mNumActiveTasks,
        mTaskPoolSize);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobWorkerHealth)) {
      return false;
    }
    JobWorkerHealth that = (JobWorkerHealth) o;
    return Objects.equal(mWorkerId, that.mWorkerId)
        && Objects.equal(mLoadAverage, that.mLoadAverage)
        && Objects.equal(mLastUpdated, that.mLastUpdated)
        && Objects.equal(mHostname, that.mHostname)
        && Objects.equal(mTaskPoolSize, that.mTaskPoolSize)
        && Objects.equal(mNumActiveTasks, that.mNumActiveTasks)
        && Objects.equal(mVersion, that.mVersion);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerId", mWorkerId)
        .add("loadAverage", mLoadAverage)
        .add("lastUpdated", mLastUpdated)
        .add("hostname", mHostname)
        .add("taskPoolSize", mTaskPoolSize)
        .add("numActiveTasks", mNumActiveTasks)
        .add("version", mVersion.getVersion())
        .add("revision", mVersion.getRevision())
        .toString();
  }
}
