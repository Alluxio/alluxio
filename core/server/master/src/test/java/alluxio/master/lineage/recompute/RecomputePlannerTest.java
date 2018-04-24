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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.lineage.meta.LineageIdGenerator;
import alluxio.master.lineage.meta.LineageStore;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Unit tests for {@link RecomputePlanner}.
 */
public final class RecomputePlannerTest {
  private RecomputePlanner mPlanner;
  private LineageStore mLineageStore;
  private Job mJob;
  private FileSystemMaster mFileSystemMaster;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
    mFileSystemMaster = mock(FileSystemMaster.class);
    when(mFileSystemMaster.getFileSystemMasterView())
        .thenReturn(new FileSystemMasterView(mFileSystemMaster));
    mPlanner = new RecomputePlanner(mLineageStore, mFileSystemMaster);
  }

  /**
   * Tests the {@link RecomputePlan#getLineageToRecompute()} method for one lost file.
   */
  @Test
  public void oneLineage() throws Exception {
    long l1 = mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    mLineageStore.createLineage(Lists.newArrayList(1L), Lists.newArrayList(2L), mJob);
    when(mFileSystemMaster.getPersistenceState(1L))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(1L));
    RecomputePlan plan = mPlanner.plan();
    assertEquals(1, plan.getLineageToRecompute().size());
    assertEquals(l1, plan.getLineageToRecompute().get(0).getId());
  }

  /**
   * Tests the {@link RecomputePlan#getLineageToRecompute()} method for two lost files.
   */
  @Test
  public void twoLostLineages() throws Exception {
    long l1 = mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    long l2 = mLineageStore.createLineage(Lists.newArrayList(1L), Lists.newArrayList(2L), mJob);
    when(mFileSystemMaster.getPersistenceState(1L))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    when(mFileSystemMaster.getPersistenceState(2L))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(1L, 2L));
    RecomputePlan plan = mPlanner.plan();
    assertEquals(2, plan.getLineageToRecompute().size());
    assertEquals(l1, plan.getLineageToRecompute().get(0).getId());
    assertEquals(l2, plan.getLineageToRecompute().get(1).getId());
  }

  /**
   * Tests the {@link RecomputePlan#getLineageToRecompute()} method for one chechpointed lineage.
   */
  @Test
  public void oneCheckointedLineage() throws Exception {
    mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    when(mFileSystemMaster.getPersistenceState(1L))
        .thenReturn(PersistenceState.PERSISTED);
    when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(1L));
    RecomputePlan plan = mPlanner.plan();
    assertEquals(0, plan.getLineageToRecompute().size());
  }

  /**
   * Tests the {@link RecomputePlan#getLineageToRecompute()} method for one lost lineage.
   */
  @Test
  public void oneLostLineage() throws Exception {
    mLineageStore.createLineage(new ArrayList<Long>(), Lists.newArrayList(1L), mJob);
    long l2 = mLineageStore.createLineage(Lists.newArrayList(1L), Lists.newArrayList(2L), mJob);
    when(mFileSystemMaster.getPersistenceState(1L))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    when(mFileSystemMaster.getPersistenceState(2L))
        .thenReturn(PersistenceState.NOT_PERSISTED);
    when(mFileSystemMaster.getLostFiles()).thenReturn(Lists.newArrayList(2L));
    RecomputePlan plan = mPlanner.plan();
    assertEquals(1, plan.getLineageToRecompute().size());
    assertEquals(l2, plan.getLineageToRecompute().get(0).getId());
  }
}
