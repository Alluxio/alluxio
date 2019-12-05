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

import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

/**
 * The job worker health reporter.
 */
public class JobWorkerHealthReporter {

  HardwareAbstractionLayer mHardware;

  /**
   * Default constructor.
   */
  public JobWorkerHealthReporter() {
    mHardware = new SystemInfo().getHardware();
  }

  /**
   * Returns the system load average of the worker.
   * See http://oshi.github.io/oshi/apidocs/oshi/hardware/CentralProcessor.html#getSystemLoadAverage
   *
   * @return the system load average of the worker
   */
  public List<Double> getCpuLoadAverage() {
    return DoubleStream.of(mHardware.getProcessor().getSystemLoadAverage(3)).boxed()
        .collect(Collectors.toList());
  }
}
