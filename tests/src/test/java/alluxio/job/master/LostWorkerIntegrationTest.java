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

import static org.junit.Assert.assertTrue;

import alluxio.conf.ServerConfiguration;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.conf.PropertyKey;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.JobWorkerIdRegistry;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests that we properly handle worker heartbeat timeouts and reregistrations.
 */
public class LostWorkerIntegrationTest extends BaseIntegrationTest {
  private static final int WORKER_HEARTBEAT_TIMEOUT_MS = 10;

  @Rule
  public ManuallyScheduleHeartbeat mSchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION,
      HeartbeatContext.JOB_WORKER_COMMAND_HANDLING);

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.JOB_MASTER_WORKER_TIMEOUT, Integer.toString(WORKER_HEARTBEAT_TIMEOUT_MS)),
      ServerConfiguration.global());

  // We need this because LocalAlluxioJobCluster doesn't work without it.
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
  }

  @Test
  public void lostWorkerReregisters() throws Exception {
    final Long initialId = JobWorkerIdRegistry.getWorkerId();
    // Sleep so that the master thinks the worker has gone too long without a heartbeat.
    CommonUtils.sleepMs(WORKER_HEARTBEAT_TIMEOUT_MS + 1);
    HeartbeatScheduler.execute(HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION);
    assertTrue(mLocalAlluxioJobCluster.getMaster().getJobMaster().getWorkerInfoList().isEmpty());

    // Reregister the worker.
    HeartbeatScheduler.execute(HeartbeatContext.JOB_WORKER_COMMAND_HANDLING);
    CommonUtils.waitFor("worker to reregister",
        () -> !mLocalAlluxioJobCluster.getMaster().getJobMaster().getWorkerInfoList().isEmpty()
            && JobWorkerIdRegistry.getWorkerId() != initialId,
        WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
  }
}
