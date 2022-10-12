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

package alluxio.client.rest;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.AlluxioJobMasterRestServiceHandler;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.security.authentication.AuthType;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.HttpMethod;

/**
 * Tests for {@link AlluxioJobMasterRestServiceHandler}.
 */
public final class JobMasterRestApiTest extends RestApiTest {
  private LocalAlluxioJobCluster mJobCluster;

  // TODO(chaomin): Rest API integration tests are only run in NOSASL mode now. Need to
  // fix the test setup in SIMPLE mode.
  @Rule
  public LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false)
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL)
      .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "1KB")
      .build();

  @Before
  public void before() throws Exception {
    mJobCluster = new LocalAlluxioJobCluster();
    mJobCluster.start();
    mHostname = mJobCluster.getHostname();
    mPort = mJobCluster.getMaster().getWebAddress().getPort();
    mBaseUri = String.format("%s/%s", mBaseUri, AlluxioJobMasterRestServiceHandler.SERVICE_PREFIX);
  }

  @After
  public void after() throws Exception {
    mJobCluster.stop();
    Configuration.reloadProperties();
  }

  @Test
  public void getInfo() throws Exception {
    new TestCase(mHostname, mPort, mBaseUri,
        AlluxioJobMasterRestServiceHandler.GET_INFO, NO_PARAMS, HttpMethod.GET,
        TestCaseOptions.defaults()).runAndGetResponse();
  }
}
