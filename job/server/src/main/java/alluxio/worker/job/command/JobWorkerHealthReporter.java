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

package alluxio.worker.job.command;

import alluxio.wire.WorkerNetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

/**
 * The job worker health reporter.
 */
public class JobWorkerHealthReporter {
  private static final Logger LOG = LoggerFactory.getLogger(JobWorkerHealthReporter.class);

  private final HardwareAbstractionLayer mHardware;
  private final WorkerNetAddress mWorkerNetAddress;

  /**
   * Creates a new instance of {@link JobWorkerHealthReporter}.
   *
   * @param workerNetAddress the connection info for this worker
   */
  public JobWorkerHealthReporter(WorkerNetAddress workerNetAddress) {
    mWorkerNetAddress = workerNetAddress;
    mHardware = new SystemInfo().getHardware();
  }

  /**
   *
   * @return instance of JobWorkerHealthReport
   */
  public JobWorkerHealthReport getJobWorkerHealthReport() {
    return new JobWorkerHealthReport(mHardware, mWorkerNetAddress);
  }

  /**
   * It represents the job worker health information.
   */
  public static class JobWorkerHealthReport{
    private static final double CPU_LOAD_AVERAGE_HEALTHY_FACTOR = 1.0;
    private final WorkerNetAddress mWorkerNetAddress;
    private final List<Double> mCpuLoadAverage;
    private final int mLogicalProcessorCount;
    private final long mLastComputed;

    /**
     * Creates a new instance of {@link JobWorkerHealthReport}.
     *
     * @param hardware represents the system info of the worker
     * @param workerNetAddress the connection info for this worker
     */
    public JobWorkerHealthReport(HardwareAbstractionLayer hardware,
                               WorkerNetAddress workerNetAddress){
      mWorkerNetAddress = workerNetAddress;
      mCpuLoadAverage = DoubleStream.of(hardware.getProcessor().getSystemLoadAverage(3)).boxed()
              .collect(Collectors.toList());
      mLogicalProcessorCount = hardware.getProcessor().getLogicalProcessorCount();
      mLastComputed = System.currentTimeMillis();
    }

    /**
     * Returns the system load average of the worker.
     * See https://www.oshi.ooo/oshi-core/apidocs/oshi/hardware/CentralProcessor.html#getSystemLoadAverage(int)
     *
     * @return the system load average of the worker
     */
    public List<Double> getCpuLoadAverage() {
      return mCpuLoadAverage;
    }

    /**
     * Returns the system logical processor count.
     * Ref: https://www.oshi.ooo/oshi-core/apidocs/oshi/hardware/CentralProcessor.html#getLogicalProcessorCount()
     *
     * @return the logical process count of the worker
     */
    public int getLogicalProcessorCount(){
      return mLogicalProcessorCount;
    }

    /**
     * Returns the worker health observed time.
     *
     * @return observed time
     */
    public long getLastComputed(){
      return mLastComputed;
    }

    /**
     * Determines whether the system is healthy from all the metrics it has collected.
     *
     * @return true if system is deemed healthy, false otherwise
     */
    public boolean isHealthy() {
      if (mCpuLoadAverage.isEmpty()) {
        // report healthy if cpu load average is not computable
        return true;
      }

      boolean isWorkerHealthy =
              mLogicalProcessorCount * CPU_LOAD_AVERAGE_HEALTHY_FACTOR > mCpuLoadAverage.get(0);

      if (!isWorkerHealthy) {
        LOG.warn("Worker,{}, is not healthy. Health-> logical process count:{}, average cpu load:{}",
                mWorkerNetAddress.getHost(),
                mLogicalProcessorCount,
                mCpuLoadAverage);
      }

      return isWorkerHealthy;
    }
  }
}
