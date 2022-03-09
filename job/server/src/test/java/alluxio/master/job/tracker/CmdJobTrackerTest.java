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

package alluxio.master.job.tracker;

import static org.mockito.Mockito.mock;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.plan.PlanDefinitionRegistry;
import alluxio.underfs.UfsManager;

import org.junit.Before;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.io.Serializable;

/**
 * Tests {@link CmdJobTrackerTest}.
 */
public final class CmdJobTrackerTest {
  private long mJobId;
  private JobConfig mJobconfig;
  private JobServerContext mJobServerContext;

  @Before
  public void before() throws Exception {

    // Create mock JobServerContext
    FileSystem fs = mock(FileSystem.class);
    FileSystemContext fsCtx = PowerMockito.mock(FileSystemContext.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    //mJobServerContext = new JobServerContext(fs, fsCtx, ufsManager);

    // Create mock job info.
    mJobconfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(mJobconfig.getName()).thenReturn("mock");
    mJobId = 1;

    // Create mock job definition.
    @SuppressWarnings("unchecked")
    PlanDefinition<JobConfig, Serializable, Serializable> mockPlanDefinition =
            Mockito.mock(PlanDefinition.class);
    PlanDefinitionRegistry singleton = PowerMockito.mock(PlanDefinitionRegistry.class);
    Whitebox.setInternalState(PlanDefinitionRegistry.class, "INSTANCE", singleton);
    Mockito.when(singleton.getJobDefinition(mJobconfig)).thenReturn(mockPlanDefinition);
    //mPlanDefinition = mockPlanDefinition;

    // Create test worker.
//    mWorkerInfo = new WorkerInfo();
//    mWorkerInfo.setId(0);
//    mWorkerInfoList = Lists.newArrayList(mWorkerInfo);
  }

  public void runDistCp() {
  }
}
