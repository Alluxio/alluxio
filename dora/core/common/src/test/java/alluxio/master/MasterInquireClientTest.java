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

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient.ConnectDetails;
import alluxio.master.SingleMasterInquireClient.SingleMasterConnectDetails;
import alluxio.master.ZkMasterInquireClient.ZkMasterConnectDetails;
import alluxio.master.journal.JournalType;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.HashMap;

/**
 * Unit tests for functionality in {@link MasterInquireClient}.
 */
public final class MasterInquireClientTest {

  private InstancedConfiguration mConfiguration;

  @Before
  public void before() {
    mConfiguration = Configuration.copyGlobal();
  }

  @Test
  public void defaultConnectString() {
    ConnectDetails cs = new SingleMasterConnectDetails(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConfiguration));
    assertCurrentConnectString(cs);
    assertEquals(NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, mConfiguration) + ":"
        + NetworkAddressUtils.getPort(ServiceType.MASTER_RPC, mConfiguration), cs.toString());
  }

  @Test
  public void singleMasterConnectString() throws Exception {
    String host = "testhost";
    int port = 123;
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.MASTER_HOSTNAME, host);
        put(PropertyKey.MASTER_RPC_PORT, port);
      }
    }, mConfiguration).toResource()) {
      ConnectDetails cs =
          new SingleMasterConnectDetails(InetSocketAddress.createUnresolved(host, port));
      assertCurrentConnectString(cs);
      assertEquals("testhost:123", cs.toString());
    }
  }

  private void assertCurrentConnectString(ConnectDetails cs) {
    assertEquals(cs, MasterInquireClient.Factory.getConnectDetails(mConfiguration));
  }
}
