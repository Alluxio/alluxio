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

package alluxio.master.lineage.recompute;

import alluxio.job.Job;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.lineage.meta.Lineage;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link RecomputeExecutor}.
 */
public final class TestRecomputeExecutor {

  /**
   * Tests recompute executor creates a recompute plan and launches the recompute job at heartbeat.
   */
  @Test
  public void recomputeLauncher() throws Exception {
    long fileId = 5L;
    // mock planner
    RecomputePlanner planner = Mockito.mock(RecomputePlanner.class);
    Job job = Mockito.mock(Job.class);
    Lineage lineage = new Lineage(1, new ArrayList<Long>(), Lists.newArrayList(fileId), job);
    Mockito.when(planner.plan()).thenReturn(new RecomputePlan(Lists.newArrayList(lineage)));

    // mock file system master
    FileSystemMaster fileSystemMaster = Mockito.mock(FileSystemMaster.class);
    Mockito.when(fileSystemMaster.getFileSystemMasterView())
        .thenReturn(new FileSystemMasterView(fileSystemMaster));
    Mockito.when(fileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(fileId));

    RecomputeExecutor executor = new RecomputeExecutor(planner, fileSystemMaster);
    // wait for the executor to finish running
    executor.heartbeatWithFuture().get(5, TimeUnit.SECONDS);
    executor.close();

    Mockito.verify(fileSystemMaster).resetFile(fileId);
    Mockito.verify(job).run();
  }
}
