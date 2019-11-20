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

import com.google.common.collect.Lists;

import java.util.List;

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
  public List<Pair<WorkerInfo, SerializableVoid>> selectExecutors(SleepJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext selectExecutorsContext)
      throws Exception {
    List<Pair<WorkerInfo, SerializableVoid>> executors = Lists.newArrayList();
    for (WorkerInfo jobWorker : jobWorkerInfoList) {
      executors.add(new Pair<>(jobWorker, null));
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
