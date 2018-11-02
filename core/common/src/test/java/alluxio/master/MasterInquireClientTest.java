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

import static org.junit.Assert.assertEquals;

import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.master.MasterInquireClient.ConnectDetails;
import alluxio.master.SingleMasterInquireClient.SingleMasterConnectDetails;
import alluxio.master.ZkMasterInquireClient.ZkMasterConnectDetails;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.HashMap;

/**
 * Unit tests for functionality in {@link MasterInquireClient}.
 */
public final class MasterInquireClientTest {
  @Test
  public void defaultConnectString() {
    ConnectDetails cs = new SingleMasterConnectDetails(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
    assertCurrentConnectString(cs);
    assertEquals(NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC) + ":"
        + NetworkAddressUtils.getPort(ServiceType.MASTER_RPC), cs.toString());
  }

  @Test
  public void singleMasterConnectString() throws Exception {
    String host = "testhost";
    int port = 123;
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.MASTER_HOSTNAME, host);
        put(PropertyKey.MASTER_RPC_PORT, Integer.toString(port));
      }
    }).toResource()) {
      ConnectDetails cs = new SingleMasterConnectDetails(new InetSocketAddress(host, port));
      assertCurrentConnectString(cs);
      assertEquals("testhost:123", cs.toString());
    }
  }

  @Test
  public void zkConnectString() throws Exception {
    String zkAddr = "zkAddr:1234";
    String leaderPath = "/my/leader/path";
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.ZOOKEEPER_ADDRESS, zkAddr);
        put(PropertyKey.ZOOKEEPER_LEADER_PATH, leaderPath);
      }
    }).toResource()) {
      ConnectDetails singleConnect = new SingleMasterConnectDetails(
          NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
      assertCurrentConnectString(singleConnect);
      try (Closeable c2 =
          new ConfigurationRule(PropertyKey.ZOOKEEPER_ENABLED, "true").toResource()) {
        ConnectDetails zkConnect = new ZkMasterConnectDetails(zkAddr, leaderPath);
        assertCurrentConnectString(zkConnect);
        assertEquals("zk@zkAddr:1234/my/leader/path", zkConnect.toString());
      }
    }
  }

  private static void assertCurrentConnectString(ConnectDetails cs) {
    assertEquals(cs, MasterInquireClient.Factory.getConnectDetails(Configuration.global()));
  }
}
