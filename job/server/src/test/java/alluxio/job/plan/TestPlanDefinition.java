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
import alluxio.job.TestPlanConfig;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple dummy job for testing.
 */
@NotThreadSafe
public final class TestPlanDefinition
    extends AbstractVoidPlanDefinition<TestPlanConfig, SerializableVoid> {

  /**
   * Constructs a new {@link TestPlanDefinition}.
   */
  public TestPlanDefinition() {}

  @Override
  public Class<TestPlanConfig> getJobConfigClass() {
    return TestPlanConfig.class;
  }

  @Override
  public SerializableVoid runTask(TestPlanConfig config, SerializableVoid args,
                                  RunTaskContext jobWorkerContext) throws Exception {
    return null;
  }

  @Override
  public Set<Pair<WorkerInfo, SerializableVoid>> selectExecutors(TestPlanConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext selectExecutorsContext)
      throws Exception {
    return null;
  }
}
