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

import alluxio.conf.ServerConfiguration;
import alluxio.master.AlluxioJobMasterRestServiceHandler;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.security.LoginUserTestUtils;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Tests for {@link AlluxioJobMasterRestServiceHandler}.
 */
public final class JobMasterRestApiTest extends RestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private LocalAlluxioJobCluster mJobCluster;
  private Map<NetworkAddressUtils.ServiceType, ServerSocket> mMasterServiceMapping;

  @Override
  public void customizeAlluxioCluster(LocalAlluxioClusterResource.Builder resource) {
    mMasterServiceMapping = IntegrationTestUtils.createMasterServiceMapping();
    resource.setSockets(mMasterServiceMapping.get(NetworkAddressUtils.ServiceType.MASTER_RPC),
        mMasterServiceMapping.get(NetworkAddressUtils.ServiceType.MASTER_WEB));
  }

  @Before
  public void before() throws Exception {
    mJobCluster = new LocalAlluxioJobCluster(mMasterServiceMapping.get(
        NetworkAddressUtils.ServiceType.JOB_MASTER_RPC),
        mMasterServiceMapping.get(NetworkAddressUtils.ServiceType.JOB_MASTER_WEB));
    mJobCluster.start();
    mHostname = mJobCluster.getHostname();
    mPort = mJobCluster.getMaster().getWebAddress().getPort();
    mServicePrefix = AlluxioJobMasterRestServiceHandler.SERVICE_PREFIX;
  }

  @After
  public void after() throws Exception {
    mJobCluster.stop();
    LoginUserTestUtils.resetLoginUser();
    ServerConfiguration.reset();
  }

  @Test
  public void getInfo() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioJobMasterRestServiceHandler.GET_INFO),
        NO_PARAMS, HttpMethod.GET, null).call();
  }
}
