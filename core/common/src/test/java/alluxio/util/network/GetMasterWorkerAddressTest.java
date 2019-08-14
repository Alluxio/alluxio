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

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Tests the {@link NetworkAddressUtils} methods related to connecting to the master and worker
 * nodes.
 */
public class GetMasterWorkerAddressTest {

  /**
   * Tests the
   * {@link NetworkAddressUtils#getConnectAddress(ServiceType, alluxio.conf.AlluxioConfiguration)}
   * method for a master node.
   */
  @Test
  public void getMasterAddress() {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();

    // connect host and port
    conf.set(PropertyKey.MASTER_HOSTNAME, "RemoteMaster1");
    conf.set(PropertyKey.MASTER_RPC_PORT, "10000");
    int resolveTimeout = (int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    String defaultHostname = NetworkAddressUtils.getLocalHostName(resolveTimeout);
    int defaultPort = Integer.parseInt(PropertyKey.MASTER_RPC_PORT.getDefaultValue());
    InetSocketAddress masterAddress =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    assertEquals(InetSocketAddress.createUnresolved("RemoteMaster1", 10000), masterAddress);
    conf = ConfigurationTestUtils.defaults();

    // port only
    conf.set(PropertyKey.MASTER_RPC_PORT, "20000");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    assertEquals(InetSocketAddress.createUnresolved(defaultHostname, 20000), masterAddress);
    conf = ConfigurationTestUtils.defaults();

    // connect host only
    conf.set(PropertyKey.MASTER_HOSTNAME, "RemoteMaster3");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    assertEquals(InetSocketAddress.createUnresolved("RemoteMaster3", defaultPort), masterAddress);
    conf = ConfigurationTestUtils.defaults();

    // all default
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    assertEquals(InetSocketAddress.createUnresolved(defaultHostname, defaultPort), masterAddress);
  }
}
