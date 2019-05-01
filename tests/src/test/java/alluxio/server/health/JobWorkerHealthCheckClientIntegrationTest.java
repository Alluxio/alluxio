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

package alluxio.server.health;

import alluxio.HealthCheckClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.job.JobWorkerHealthCheckClient;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.retry.CountingRetry;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;

public class JobWorkerHealthCheckClientIntegrationTest extends BaseIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setProperty(PropertyKey.JOB_WORKER_RPC_PORT, 0)
          .build();

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster = null;
  private HealthCheckClient mHealthCheckClient;

  @Before
  public final void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    InetSocketAddress address = mLocalAlluxioJobCluster.getWorker().getRpcAddress();
    mHealthCheckClient = new JobWorkerHealthCheckClient(address, () -> new CountingRetry(1),
        ServerConfiguration.global());
  }

  @After
  public final void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
  }

  @Test
  public void isServing() {
    Assert.assertTrue(mHealthCheckClient.isServing());
  }

  @Test
  public void isServingStopJobs() throws Exception {
    mLocalAlluxioJobCluster.stop();
    Assert.assertFalse(mHealthCheckClient.isServing());
  }
}
