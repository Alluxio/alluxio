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
import alluxio.job.CrashPlanConfig;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * This definition of a plan that does nothing.
 */
public class CrashPlanDefinition
    extends AbstractVoidPlanDefinition<CrashPlanConfig, SerializableVoid> {

  public CrashPlanDefinition() {}

  @Override
  public Class<CrashPlanConfig> getJobConfigClass() {
    return CrashPlanConfig.class;
  }

  @Override
  public Set<Pair<WorkerInfo, SerializableVoid>> selectExecutors(CrashPlanConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext selectExecutorsContext)
      throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> executors =
        Collections.newSetFromMap(new IdentityHashMap<>());
    executors.add(new Pair<>(jobWorkerInfoList.get(0), null));
    return executors;
  }

  @Override
  public SerializableVoid runTask(CrashPlanConfig config, SerializableVoid args,
                                  RunTaskContext runTaskContext) throws Exception {
    throw new IllegalStateException("CrashPlanConfig always crashes");
  }
}
