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

package alluxio.util.network;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Tests the {@link NetworkAddressUtils} methods related to connecting to the master and worker
 * nodes.
 */
public class GetMasterWorkerAddressTest {

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests the {@link NetworkAddressUtils#getConnectAddress(ServiceType)} method for
   * a master node.
   */
  @Test
  public void getMasterAddressTest() {
    // connect host and port
    Configuration.set(Constants.MASTER_HOSTNAME, "RemoteMaster1");
    Configuration.set(Constants.MASTER_RPC_PORT, "10000");
    String defaultHostname = NetworkAddressUtils.getLocalHostName();
    int defaultPort = Constants.DEFAULT_MASTER_PORT;
    InetSocketAddress masterAddress =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);
    Assert.assertEquals(new InetSocketAddress("RemoteMaster1", 10000), masterAddress);
    ConfigurationTestUtils.resetConfiguration();

    // port only
    Configuration.set(Constants.MASTER_RPC_PORT, "20000");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, 20000), masterAddress);
    ConfigurationTestUtils.resetConfiguration();

    // connect host only
    Configuration.set(Constants.MASTER_HOSTNAME, "RemoteMaster3");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);
    Assert.assertEquals(new InetSocketAddress("RemoteMaster3", defaultPort), masterAddress);
    ConfigurationTestUtils.resetConfiguration();

    // all default
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, defaultPort), masterAddress);
  }

  /**
   * Tests the {@link NetworkAddressUtils#getConnectAddress(ServiceType)} method for
   * a worker node.
   */
  @Test
  public void getWorkerAddressTest() {
    // port only
    Configuration.set(Constants.WORKER_RPC_PORT, "10001");
    String defaultHostname = NetworkAddressUtils.getLocalHostName();
    int defaultPort = Constants.DEFAULT_WORKER_PORT;
    InetSocketAddress workerAddress =
        NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, 10001), workerAddress);
    ConfigurationTestUtils.resetConfiguration();

    // all default
    workerAddress = NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, defaultPort), workerAddress);
  }
}
