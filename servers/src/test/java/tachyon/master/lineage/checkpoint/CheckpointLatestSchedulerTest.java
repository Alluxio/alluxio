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
  private CheckpointLatestScheduler mScheduler;

  @Before
  public void before() {
    mLineageStore = new LineageStore(new LineageIdGenerator());
    mJob = new CommandLineJob("test", new JobConf("output"));
    mScheduler = new CheckpointLatestScheduler(new LineageStoreView(mLineageStore));
  }

  @Test
  public void scheduleTest() throws Exception {
    long l1 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(),
        Lists.newArrayList(new LineageFile(1)), mJob);
    Thread.sleep(1);
    long l2 = mLineageStore.createLineage(Lists.<TachyonFile>newArrayList(new TachyonFile(1)),
        Lists.newArrayList(new LineageFile(2)), mJob);
    // complete first
    mLineageStore.completeFile(1, "test1");

    CheckpointPlan plan = mScheduler.schedule(new LineageStoreView(mLineageStore));
    Assert.assertEquals((Long) l1, plan.getLineagesToCheckpoint().get(0));

    // complete second
    mLineageStore.completeFile(2, "test2");
    plan = mScheduler.schedule(new LineageStoreView(mLineageStore));
    Assert.assertEquals((Long) l2, plan.getLineagesToCheckpoint().get(0));
  }
}
