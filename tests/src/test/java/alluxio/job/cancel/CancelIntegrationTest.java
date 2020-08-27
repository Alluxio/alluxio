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
import alluxio.job.SleepJobConfig;
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
  
  @Test
  public void cancelTest() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(5000));
    waitForJobRunning(jobId);
    // cancel the job
    mJobMaster.cancel(jobId);
    waitForJobCancelled(jobId);
  }
}
