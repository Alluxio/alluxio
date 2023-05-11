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

package alluxio.job.plan;

import alluxio.collections.Pair;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.SleepJobConfig;
import alluxio.job.util.SerializableVoid;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * The definition for a job which sleeps for the specified number of milliseconds on each worker.
 */
public final class SleepPlanDefinition
    extends AbstractVoidPlanDefinition<SleepJobConfig, SerializableVoid> {

  /**
   * Constructs a new {@link SleepPlanDefinition}.
   */
  public SleepPlanDefinition() {}

  @Override
  public Class<SleepJobConfig> getJobConfigClass() {
    return SleepJobConfig.class;
  }

  @Override
  public Set<Pair<WorkerInfo, SerializableVoid>> selectExecutors(SleepJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext selectExecutorsContext)
      throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> executors =
        Collections.newSetFromMap(new IdentityHashMap<>());
    for (WorkerInfo jobWorker : jobWorkerInfoList) {
      for (int i = 0; i < config.getTasksPerWorker(); i++) {
        executors.add(new Pair<>(jobWorker, null));
      }
    }
    return executors;
  }

  @Override
  public SerializableVoid runTask(SleepJobConfig config, SerializableVoid args,
      RunTaskContext runTaskContext) throws Exception {
    CommonUtils.sleepMs(config.getTimeMs());
    return null;
  }
}
