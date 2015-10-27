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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import tachyon.client.file.TachyonFile;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageIdGenerator;
import tachyon.master.lineage.meta.LineageStore;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class RecomputePlannerTest {
  private RecomputePlanner mPlanner;
  private LineageStore mLineageStore;
  private Job mJob;
  private FileSystemMaster mFileSystemMaster;

  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mPlanner = new RecomputePlanner(mLineageStore, mFileSystemMaster);
  }

  @Test
  public void oneLineageTest() {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    mLineageStore.completeFile(1);
    Mockito.when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(1L));
    RecomputePlan plan = mPlanner.plan();
    Assert.assertEquals(1, plan.getLineageToRecompute().size());
    Assert.assertEquals(l1, plan.getLineageToRecompute().get(0).getId());
  }

  @Test
  public void twoLostLineagesTest() {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    mLineageStore.completeFile(1);
    mLineageStore.completeFile(2);
    Mockito.when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(1L, 2L));
    RecomputePlan plan = mPlanner.plan();
    Assert.assertEquals(2, plan.getLineageToRecompute().size());
    Assert.assertEquals(l1, plan.getLineageToRecompute().get(0).getId());
    Assert.assertEquals(l2, plan.getLineageToRecompute().get(1).getId());
  }

  @Test
  public void oneCheckointedLineageTest() {
    mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1L)), mJob);
    mLineageStore.completeFile(1L);
    mLineageStore.commitFilePersistence(1L);
    Mockito.when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(1L));
    RecomputePlan plan = mPlanner.plan();
    Assert.assertEquals(0, plan.getLineageToRecompute().size());
  }

  @Test
  public void oneLostLineageTest() {
    mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    mLineageStore.completeFile(1);
    mLineageStore.completeFile(2);
    Mockito.when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(2L));
    RecomputePlan plan = mPlanner.plan();
    Assert.assertEquals(1, plan.getLineageToRecompute().size());
    Assert.assertEquals(l2, plan.getLineageToRecompute().get(0).getId());
  }
}
