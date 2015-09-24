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

import com.google.common.collect.Lists;

import tachyon.client.file.TachyonFile;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageIdGenerator;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.meta.LineageStoreView;

public final class CheckpointLatestSchedulerTest {
  private LineageStore mLineageStore;
  private Job mJob;
  private CheckpointLatestScheduler scheduler;

  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
    scheduler = new CheckpointLatestScheduler(new LineageStoreView(mLineageStore));
  }

  @Test
  public void scheduleTest() {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    // complete first
    mLineageStore.completeFileForAsyncWrite(1, "test1");

    CheckpointPlan plan = scheduler.schedule(new LineageStoreView(mLineageStore));
    Assert.assertEquals(l1, plan.getLineagesToCheckpoint().get(0).getId());

    // complete second
    mLineageStore.completeFileForAsyncWrite(2, "test2");
    plan = scheduler.schedule(new LineageStoreView(mLineageStore));
    Assert.assertEquals(l2, plan.getLineagesToCheckpoint().get(0).getId());
  }
}
