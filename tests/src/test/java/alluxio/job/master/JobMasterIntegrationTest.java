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

package alluxio.job.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.plan.PlanDefinitionRegistryRule;
import alluxio.job.SleepJobConfig;
import alluxio.job.plan.SleepPlanDefinition;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.workflow.composite.CompositeConfig;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerInfo;
import alluxio.worker.JobWorkerProcess;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for the job master.
 */
public final class JobMasterIntegrationTest extends BaseIntegrationTest {
  private static final long WORKER_TIMEOUT_MS = 500;
  private static final long LOST_WORKER_INTERVAL_MS = 500;
  private JobMaster mJobMaster;
  private JobWorkerProcess mJobWorker;
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, 20)
          .setProperty(PropertyKey.JOB_MASTER_WORKER_TIMEOUT, WORKER_TIMEOUT_MS)
          .setProperty(PropertyKey.JOB_MASTER_LOST_WORKER_INTERVAL, LOST_WORKER_INTERVAL_MS)
          .build();

  @Rule
  public PlanDefinitionRegistryRule mJobRule =
      new PlanDefinitionRegistryRule(SleepJobConfig.class, new SleepPlanDefinition());

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mJobWorker = mLocalAlluxioJobCluster.getWorker();
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
  }

  @Test
  public void multipleTasksPerWorker() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1, 2));

    JobInfo jobStatus = mJobMaster.getStatus(jobId);
    assertEquals(2, jobStatus.getChildren().size());

    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);

    jobStatus = mJobMaster.getStatus(jobId);
    assertEquals(2, jobStatus.getChildren().size());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "1",
      PropertyKey.Name.JOB_MASTER_FINISHED_JOB_RETENTION_TIME, "0"})
  public void flowControl() throws Exception {
    for (int i = 0; i < 10; i++) {
      while (true) {
        try {
          mJobMaster.run(new SleepJobConfig(100));
          break;
        } catch (ResourceExhaustedException e) {
          // sleep for a little before retrying the job
          CommonUtils.sleepMs(100);
        }
      }
    }
  }

  @Test
  public void restartMasterAndLoseWorker() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    CommonUtils.waitFor("Worker to register with restarted job master",
        () -> !mJobMaster.getWorkerInfoList().isEmpty(),
        WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
    mJobWorker.stop();
    CommonUtils.sleepMs(WORKER_TIMEOUT_MS + LOST_WORKER_INTERVAL_MS);
    assertTrue(mJobMaster.getWorkerInfoList().isEmpty());
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.JOB_MASTER_LOST_WORKER_INTERVAL, "10000000"})
  public void restartMasterAndReregisterWorker() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    CommonUtils.waitFor("Worker to register with restarted job master",
        () -> !mJobMaster.getWorkerInfoList().isEmpty(),
        WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
    final long firstWorkerId = mJobMaster.getWorkerInfoList().get(0).getId();
    mLocalAlluxioJobCluster.restartWorker();
    CommonUtils.waitFor("Restarted worker to register with job master", () -> {
      List<WorkerInfo> workerInfo = mJobMaster.getWorkerInfoList();
      return !workerInfo.isEmpty() && workerInfo.get(0).getId() != firstWorkerId;
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
    // The restarted worker should replace the original worker since they have the same address.
    assertEquals(1, mJobMaster.getWorkerInfoList().size());
  }

  @Test
  public void getAllWorkerHealth() throws Exception {
    final AtomicReference<List<JobWorkerHealth>> singleton = new AtomicReference<>();
    CommonUtils.waitFor("allWorkerHealth", () -> {
      List<JobWorkerHealth> allWorkerHealth = mJobMaster.getAllWorkerHealth();
      singleton.set(allWorkerHealth);
      return allWorkerHealth.size() == 1;
    });
    List<JobWorkerHealth> allWorkerHealth = singleton.get();

    JobWorkerHealth workerHealth = allWorkerHealth.get(0);
    assertNotNull(workerHealth.getHostname());
    assertEquals(3, workerHealth.getLoadAverage().size());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "20"})
  public void stopJobWorkerTasks() throws Exception {
    long jobId0 = mJobMaster.run(new SleepJobConfig(5000));
    long jobId1 = mJobMaster.run(new SleepJobConfig(5000));
    long jobId2 = mJobMaster.run(new SleepJobConfig(1));
    long jobId3 = mJobMaster.run(new SleepJobConfig(1));

    JobTestUtils.waitForJobStatus(mJobMaster, jobId2, Status.COMPLETED);
    JobTestUtils.waitForJobStatus(mJobMaster, jobId3, Status.COMPLETED);

    assertFalse(mJobMaster.getStatus(jobId1).getStatus().isFinished());
    assertFalse(mJobMaster.getStatus(jobId0).getStatus().isFinished());
    assertEquals(2, mJobMaster.getAllWorkerHealth().get(0).getNumActiveTasks());

    mJobMaster.setTaskPoolSize(0);
    long jobId5 = mJobMaster.run(new SleepJobConfig(1));

    // wait to make sure that this job is not completing any time soon
    CommonUtils.sleepMs(300);
    assertFalse(mJobMaster.getStatus(jobId5).getStatus().isFinished());
    assertEquals(0, mJobMaster.getAllWorkerHealth().get(0).getTaskPoolSize());

    // existing running tasks will continue to run
    assertEquals(2, mJobMaster.getAllWorkerHealth().get(0).getNumActiveTasks());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "20"})
  public void throttleJobWorkerTasks() throws Exception {
    mJobMaster.setTaskPoolSize(1);

    long jobId0 = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId0,
        Sets.newHashSet(Status.RUNNING, Status.COMPLETED));

    long jobId1 = mJobMaster.run(new SleepJobConfig(50000));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId1, Status.RUNNING);
    JobTestUtils.waitForJobStatus(mJobMaster, jobId0, Status.COMPLETED);

    long jobId2 = mJobMaster.run(new SleepJobConfig(1));
    long jobId3 = mJobMaster.run(new SleepJobConfig(1));

    // wait a bit more to make sure other jobs aren't completing
    CommonUtils.sleepMs(300);

    assertEquals(Status.RUNNING, mJobMaster.getStatus(jobId1).getStatus());
    assertEquals(Status.CREATED, mJobMaster.getStatus(jobId2).getStatus());
    assertEquals(Status.CREATED, mJobMaster.getStatus(jobId3).getStatus());

    assertEquals(1, mJobMaster.getAllWorkerHealth().get(0).getTaskPoolSize());
    assertEquals(1, mJobMaster.getAllWorkerHealth().get(0).getNumActiveTasks());

    mJobMaster.cancel(jobId1);

    JobTestUtils.waitForJobStatus(mJobMaster, jobId2, Status.COMPLETED);
    JobTestUtils.waitForJobStatus(mJobMaster, jobId3, Status.COMPLETED);
  }

  @Test
  public void cancel() throws Exception {
    SleepJobConfig childJob1 = new SleepJobConfig(50000);
    SleepJobConfig childJob2 = new SleepJobConfig(45000);
    SleepJobConfig childJob3 = new SleepJobConfig(40000);

    CompositeConfig jobConfig = new CompositeConfig(
        Lists.newArrayList(childJob1, childJob2, childJob3), false);

    long jobId = mJobMaster.run(jobConfig);

    JobInfo status = mJobMaster.getStatus(jobId);
    List<JobInfo> children = status.getChildren();

    assertEquals(3, children.size());

    long child0 = children.get(0).getId();
    long child1 = children.get(1).getId();
    long child2 = children.get(2).getId();

    mJobMaster.cancel(jobId);
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.CANCELED);
    JobTestUtils.waitForJobStatus(mJobMaster, child0, Status.CANCELED);
    JobTestUtils.waitForJobStatus(mJobMaster, child1, Status.CANCELED);
    JobTestUtils.waitForJobStatus(mJobMaster, child2, Status.CANCELED);
  }
}
