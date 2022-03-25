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
import alluxio.job.cmd.load.LoadCliConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.plan.PlanTracker;
import alluxio.underfs.UfsManager;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.Collections;
import java.util.Map;

/**
 * Tests {@link CmdJobTrackerTest}.
 */
public final class CmdJobTrackerTest {
  private long mJobId;
  private JobConfig mJobconfig;
  private JobServerContext mJobServerContext;
  private CmdJobTracker mCmdJobTracker;
  private JobMaster mJobMaster;
  private PlanTracker mPlanTracker;
  private LoadCliConfig mLoad;
  private MigrateCliConfig mMigrate;
  private final Map<Long, CmdInfo> mInfoMap = Maps.newHashMap();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws Exception {

    // Create mock JobServerContext
    FileSystem fs = mock(FileSystem.class);
    FileSystemContext fsCtx = PowerMockito.mock(FileSystemContext.class);
    mJobMaster = mock(JobMaster.class);
    mPlanTracker = mock(PlanTracker.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    //mJobServerContext = new JobServerContext(fs, fsCtx, ufsManager);

    mCmdJobTracker = new CmdJobTracker(fsCtx, mJobMaster, mPlanTracker);

    mLoad = mock(LoadCliConfig.class);
    mMigrate = mock(MigrateCliConfig.class);

    mLoad = new LoadCliConfig("/path/to/load", 3, 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, true);
    mMigrate = new MigrateCliConfig("/path/from", "/path/to", "THROUGH", true, 2);

    // Create mock job info.
    mJobconfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(mJobconfig.getName()).thenReturn("mock");
    mJobId = 1;

//    // Create mock job definition.
//    @SuppressWarnings("unchecked")
//    PlanDefinition<JobConfig, Serializable, Serializable> mockPlanDefinition =
//            Mockito.mock(PlanDefinition.class);
//    PlanDefinitionRegistry singleton = PowerMockito.mock(PlanDefinitionRegistry.class);
//    Whitebox.setInternalState(PlanDefinitionRegistry.class, "INSTANCE", singleton);
//    Mockito.when(singleton.getJobDefinition(mJobconfig)).thenReturn(mockPlanDefinition);
  }

  public void runDistributedCommandTest() throws Exception {
  }
}
