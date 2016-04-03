/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.lineage.checkpoint;

import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.lineage.meta.LineageIdGenerator;
import alluxio.master.lineage.meta.LineageStore;
import alluxio.master.lineage.meta.LineageStoreView;
import alluxio.util.CommonUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link CheckpointLatestPlanner}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class CheckpointLatestPlannerTest {
  private LineageStore mLineageStore;
  private FileSystemMaster mFileSystemMaster;
  private Job mJob;
  private CheckpointLatestPlanner mPlanner;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mPlanner = new CheckpointLatestPlanner(new LineageStoreView(mLineageStore),
        new FileSystemMasterView(mFileSystemMaster));
  }

  /**
   * Tests the {@link CheckpointLatestPlanner#generatePlan(LineageStoreView, FileSystemMasterView)}
   * method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void scheduleTest() throws Exception {
    long fileId1 = 1L;
    long fileId2 = 2L;
    long l1 =
        mLineageStore.createLineage(Lists.<Long>newArrayList(), Lists.newArrayList(fileId1), mJob);
    // Sleep for 1ms to guarantee that the next lineage's creation time is later than the first's
    CommonUtils.sleepMs(1);
    long l2 =
        mLineageStore.createLineage(Lists.newArrayList(fileId1), Lists.newArrayList(fileId2), mJob);

    Mockito.when(mFileSystemMaster.getPersistenceState(fileId1))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    Mockito.when(mFileSystemMaster.getPersistenceState(fileId2))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    FileInfo fileInfo1 = new FileInfo();
    fileInfo1.setCompleted(true);
    Mockito.when(mFileSystemMaster.getFileInfo(fileId1)).thenReturn(fileInfo1);
    FileInfo fileInfo2 = new FileInfo();
    fileInfo2.setCompleted(false);
    Mockito.when(mFileSystemMaster.getFileInfo(fileId2)).thenReturn(fileInfo2);

    CheckpointPlan plan = mPlanner.generatePlan(new LineageStoreView(mLineageStore),
        new FileSystemMasterView(mFileSystemMaster));
    Assert.assertEquals((Long) l1, plan.getLineagesToCheckpoint().get(0));

    // complete file 2 and it's ready for checkpoint
    fileInfo2.setCompleted(true);
    plan = mPlanner.generatePlan(new LineageStoreView(mLineageStore),
        new FileSystemMasterView(mFileSystemMaster));
    Assert.assertEquals((Long) l2, plan.getLineagesToCheckpoint().get(0));
  }
}
