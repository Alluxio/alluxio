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

package alluxio.job;

import alluxio.job.util.SerializableVoid;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The definition for a job which sleeps for the specified number of milliseconds on each worker.
 */
public final class SleepJobDefinition
    extends AbstractVoidJobDefinition<SleepJobConfig, SerializableVoid> {

  /**
   * Constructs a new {@link SleepJobDefinition}.
   */
  public SleepJobDefinition() {}

  @Override
  public Class<SleepJobConfig> getJobConfigClass() {
    return SleepJobConfig.class;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(SleepJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, SerializableVoid> executors = new HashMap<>();
    for (WorkerInfo jobWorker : jobWorkerInfoList) {
      executors.put(jobWorker, null);
    }
    return executors;
  }

  @Override
  public SerializableVoid runTask(SleepJobConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    CommonUtils.sleepMs(config.getTimeMs());
    return null;
  }
}
