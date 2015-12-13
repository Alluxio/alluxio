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

package tachyon.master.lineage.checkpoint;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.file.meta.FilePersistenceState;
import tachyon.master.file.meta.FileStoreView;
import tachyon.master.lineage.meta.LineageIdGenerator;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.meta.LineageStoreView;
import tachyon.thrift.FileInfo;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class CheckpointLatestSchedulerTest {
  private LineageStore mLineageStore;
  private FileSystemMaster mFileSystemMaster;
  private Job mJob;
  private CheckpointLatestScheduler mScheduler;

  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mScheduler = new CheckpointLatestScheduler(new LineageStoreView(mLineageStore),
        new FileStoreView(mFileSystemMaster));
  }

  @Test
  public void scheduleTest() throws Exception {
    long fileId1 = 1L;
    long fileId2 = 2L;
    long l1 = mLineageStore.createLineage(Lists.<Long>newArrayList(), Lists.newArrayList(fileId1), mJob);
    long l2 = mLineageStore.createLineage(Lists.newArrayList(fileId1), Lists.newArrayList(fileId2), mJob);

    Mockito.when(mFileSystemMaster.getFilePersistenceState(fileId1))
        .thenReturn(FilePersistenceState.NOT_PERSISTED);
    Mockito.when(mFileSystemMaster.getFilePersistenceState(fileId2))
        .thenReturn(FilePersistenceState.NOT_PERSISTED);
    FileInfo fileInfo1 = new FileInfo();
    fileInfo1.isCompleted=true;
    fileInfo1.isLost=false;
    Mockito.when(mFileSystemMaster.getFileInfo(fileId1)).thenReturn(fileInfo1);
    FileInfo fileInfo2 = new FileInfo();
    fileInfo2.isCompleted=false;
    fileInfo1.isLost=false;
    Mockito.when(mFileSystemMaster.getFileInfo(fileId2)).thenReturn(fileInfo2);

    CheckpointPlan plan = mScheduler.schedule(new LineageStoreView(mLineageStore),
        new FileStoreView(mFileSystemMaster));
    Assert.assertEquals((Long) l1, plan.getLineagesToCheckpoint().get(0));

    // complete file 2 and it's ready for checkpoint
    fileInfo2.isCompleted=true;
    plan = mScheduler.schedule(new LineageStoreView(mLineageStore),
        new FileStoreView(mFileSystemMaster));
    Assert.assertEquals((Long) l2, plan.getLineagesToCheckpoint().get(0));
  }
}
