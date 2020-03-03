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

package alluxio.job.cancel;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.plan.PlanConfig;
import alluxio.job.plan.PlanDefinitionRegistry;
import alluxio.job.JobIntegrationTest;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.beust.jcommander.internal.Sets;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Set;

/**
 * Tests the cancellation of a job.
 */
public final class CancelIntegrationTest extends JobIntegrationTest {
  static class CancelTestConfig implements PlanConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "Cancel";
    }
  }

  public static class CancelTestDefinition
      extends AbstractVoidPlanDefinition<CancelTestConfig, Integer> {
    @Override
    public Set<Pair<WorkerInfo, Integer>> selectExecutors(CancelTestConfig config,
        List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext selectExecutorsContext)
        throws Exception {
      Set<Pair<WorkerInfo, Integer>> result = Sets.newHashSet();
      for (WorkerInfo info : jobWorkerInfoList) {
        result.add(new Pair<>(info, 0));
      }
      return result;
    }

    @Override
    public SerializableVoid runTask(CancelTestConfig config, Integer args,
        RunTaskContext runTaskContext) throws Exception {
      // wait until interruption
      Thread.sleep(1000 * Constants.SECOND_MS);
      return null;
    }

    @Override
    public Class<CancelTestConfig> getJobConfigClass() {
      return CancelTestConfig.class;
    }
  }

  @Test(timeout = 10000)
  public void cancelTest() throws Exception {
    // register the job
    Whitebox.invokeMethod(PlanDefinitionRegistry.INSTANCE, "add", CancelTestConfig.class,
        new CancelTestDefinition());
    long jobId = mJobMaster.run(new CancelTestConfig());
    waitForJobRunning(jobId);
    // cancel the job
    mJobMaster.cancel(jobId);
    waitForJobCancelled(jobId);
  }
}
