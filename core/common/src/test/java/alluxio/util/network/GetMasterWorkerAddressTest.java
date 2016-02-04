/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.util.network;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import alluxio.Constants;
import alluxio.conf.TachyonConf;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

/**
 * Tests the {@link NetworkAddressUtils} methods related to connecting to the master and worker
 * nodes.
 */
public class GetMasterWorkerAddressTest {

  /**
   * Tests the {@link NetworkAddressUtils#getConnectAddress(ServiceType, TachyonConf)} method for a
   * master node.
   */
  @Test
  public void getMasterAddressTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.MASTER_HOSTNAME, "RemoteMaster1");
    conf.set(Constants.MASTER_RPC_PORT, "10000");
    String defaultHostname = NetworkAddressUtils.getLocalHostName(conf);
    int defaultPort = Constants.DEFAULT_MASTER_PORT;

    // connect host and port
    InetSocketAddress masterAddress =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("RemoteMaster1", 10000), masterAddress);

    conf = new TachyonConf();
    conf.set(Constants.MASTER_RPC_PORT, "20000");
    // port only
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, 20000), masterAddress);

    conf = new TachyonConf();
    conf.set(Constants.MASTER_HOSTNAME, "RemoteMaster3");
    // connect host only
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("RemoteMaster3", defaultPort), masterAddress);

    conf = new TachyonConf();
    // all default
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, defaultPort), masterAddress);
  }

  /**
   * Tests the {@link NetworkAddressUtils#getConnectAddress(ServiceType, TachyonConf)} method for a
   * worker node.
   */
  @Test
  public void getWorkerAddressTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_RPC_PORT, "10001");

    String defaultHostname = NetworkAddressUtils.getLocalHostName(conf);
    int defaultPort = Constants.DEFAULT_WORKER_PORT;

    // port only
    InetSocketAddress workerAddress =
        NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, 10001), workerAddress);

    conf = new TachyonConf();
    // all default
    workerAddress = NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, defaultPort), workerAddress);
  }
}
