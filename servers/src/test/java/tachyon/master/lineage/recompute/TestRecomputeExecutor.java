/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage.recompute;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import tachyon.job.Job;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.file.meta.FileSystemMasterView;
import tachyon.master.lineage.meta.Lineage;

/**
 * Unit tests for {@link RecomputeExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSystemMaster.class)
public final class TestRecomputeExecutor {

  /**
   * Tests recompute executor creates a recompute plan and launches the recompute job at heartbeat.
   *
   * @throws Exception if anything wrong happens
   */
  @Test
  public void recomputeLauncherTest() throws Exception {
    long fileId = 5L;
    // mock planner
    RecomputePlanner planner = Mockito.mock(RecomputePlanner.class);
    Job job = Mockito.mock(Job.class);
    Lineage lineage = new Lineage(1, Lists.<Long>newArrayList(), Lists.newArrayList(fileId), job);
    Mockito.when(planner.plan()).thenReturn(new RecomputePlan(Lists.newArrayList(lineage)));

    // mock file system master
    FileSystemMaster fileSystemMaster = Mockito.mock(FileSystemMaster.class);
    Mockito.when(fileSystemMaster.getFileSystemMasterView())
        .thenReturn(new FileSystemMasterView(fileSystemMaster));
    Mockito.when(fileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(fileId));

    RecomputeExecutor executor = new RecomputeExecutor(planner, fileSystemMaster);
    // wait for the executor to finish running
    executor.heartbeatWithFuture().get(1, TimeUnit.SECONDS);

    Mockito.verify(fileSystemMaster).resetFile(fileId);
    Mockito.verify(job).run();
  }
}
