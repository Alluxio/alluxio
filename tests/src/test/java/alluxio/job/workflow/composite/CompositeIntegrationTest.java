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

package alluxio.job.workflow.composite;

import static org.junit.Assert.assertEquals;

import alluxio.job.plan.NoopPlanConfig;
import alluxio.job.JobIntegrationTest;
import alluxio.job.SleepJobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

/**
 * Integration tests for {@link CompositeExecution}.
 */
public class CompositeIntegrationTest extends JobIntegrationTest {

  @Test
  public void simpleTest() throws Exception {
    CompositeConfig config = new CompositeConfig(
        Lists.newArrayList(new SleepJobConfig(1), new SleepJobConfig(1)), false);

    waitForJobToFinish(mJobMaster.run(config));
  }

  @Test
  public void nestedTest() throws Exception {
    CompositeConfig config = new CompositeConfig(
        Lists.newArrayList(
            new CompositeConfig(
                Lists.newArrayList(new SleepJobConfig(2), new SleepJobConfig(3)), false),
            new SleepJobConfig(1)
        ),
        true
    );

    long jobId = mJobMaster.run(config);
    waitForJobToFinish(jobId);

    JobInfo status = mJobMaster.getStatus(jobId);

    assertEquals(Status.COMPLETED, status.getStatus());
    List<JobInfo> children = status.getChildren();
    assertEquals(2, children.size());

    JobInfo child0 = children.get(0);
    assertEquals(child0, mJobMaster.getStatus(children.get(0).getId()));
    assertEquals(Status.COMPLETED, child0.getStatus());
    assertEquals(2, child0.getChildren().size());

    JobInfo child1 = children.get(1);
    assertEquals(child1, mJobMaster.getStatus(children.get(1).getId()));
    assertEquals(Status.COMPLETED, child1.getStatus());
    assertEquals(1, child1.getChildren().size());
  }

  @Test
  public void waitSequential() throws Exception {
    CompositeConfig config = new CompositeConfig(
        Lists.newArrayList(new SleepJobConfig(1000000), new SleepJobConfig(1)), true);

    long jobId = mJobMaster.run(config);

    JobInfo status = mJobMaster.getStatus(jobId);

    assertEquals(Status.RUNNING, status.getStatus());
    assertEquals(1, status.getChildren().size());
  }

  @Test
  public void waitParallel() throws Exception {
    CompositeConfig config = new CompositeConfig(
        Lists.newArrayList(new SleepJobConfig(1000000), new SleepJobConfig(1)), false);

    long jobId = mJobMaster.run(config);

    JobInfo status = mJobMaster.getStatus(jobId);

    assertEquals(Status.RUNNING, status.getStatus());
    assertEquals(2, status.getChildren().size());
  }

  @Test
  public void testCompositeDoNothing() throws Exception {
    NoopPlanConfig jobConfig = new NoopPlanConfig();

    long jobId = mJobMaster.run(new CompositeConfig(Lists.newArrayList(jobConfig), true));

    waitForJobToFinish(jobId);
  }
}
