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
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.master.LocalAlluxioCluster;
import alluxio.proxy.ProxyHealthCheckClient;
import alluxio.retry.CountingRetry;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ProxyHealthCheckClientIntegrationTest extends BaseIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
          new LocalAlluxioClusterResource.Builder()
                  .setProperty(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY, 30).build();

  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private HealthCheckClient mHealthCheckClient;

  @Before
  public final void before() throws Exception {
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mHealthCheckClient = new ProxyHealthCheckClient(
        NetworkAddressUtils.getBindAddress(NetworkAddressUtils.ServiceType.PROXY_WEB),
        () -> new CountingRetry(1));
  }

  @Test
  public void isServing() {
    Assert.assertTrue(mHealthCheckClient.isServing());
  }

  @Test
  public void isServingStopFS() throws Exception {
    mLocalAlluxioCluster.stopFS();
    Assert.assertFalse(mHealthCheckClient.isServing());
  }
}
