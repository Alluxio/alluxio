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

package alluxio.master;

import alluxio.BaseIntegrationTest;
import alluxio.HealthCheckClient;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MasterHealthCheckClientIntegrationTest extends BaseIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
          new LocalAlluxioClusterResource.Builder()
                  .setProperty(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY, 5).build();

  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private HealthCheckClient mHealthCheckClient;

  @Before
  public final void before() {
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mHealthCheckClient = new MasterHealthCheckClient.Builder()
            .withProcessCheck(false).build();
  }

  @Test
  public void isServing() {
    Assert.assertTrue(mHealthCheckClient.isServing());
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_RPC_RETRY_MAX_DURATION, "1s"})
  public void isServingStopFS() throws Exception {
    mLocalAlluxioCluster.stopFS();
    Assert.assertFalse(mHealthCheckClient.isServing());
  }
}
