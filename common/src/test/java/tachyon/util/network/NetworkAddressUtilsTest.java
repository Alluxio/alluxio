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

package tachyon.util.network;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

public class NetworkAddressUtilsTest {

  @Test
  public void testGetConnectAddress() throws Exception {
    TachyonConf conf = new TachyonConf();
    String localHostName = NetworkAddressUtils.getLocalHostName(conf);
    int defaultPort = Constants.DEFAULT_MASTER_PORT;
    InetSocketAddress masterAddress;

    // all default
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, defaultPort), masterAddress);

    // bind host only
    conf.set(Constants.MASTER_HOSTNAME, "");
    conf.set(Constants.MASTER_BIND_HOST, "master.bind.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.bind.host", defaultPort), masterAddress);

    // connect host and bind host
    conf.set(Constants.MASTER_HOSTNAME, "master.connect.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.connect.host", defaultPort), masterAddress);

    // wildcard connect host and bind host
    conf.set(Constants.MASTER_HOSTNAME, Constants.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.bind.host", defaultPort), masterAddress);

    // wildcard connect host and wildcard bind host
    conf.set(Constants.MASTER_BIND_HOST, Constants.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, defaultPort), masterAddress);

    // connect host and wildcard bind host
    conf.set(Constants.MASTER_HOSTNAME, "master.connect.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.connect.host", defaultPort), masterAddress);

    // connect host and wildcard bind host with port
    conf.set(Constants.MASTER_PORT, "10000");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.connect.host", 10000), masterAddress);

    // connect host and bind host with port
    conf.set(Constants.MASTER_BIND_HOST, "master.bind.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.connect.host", 10000), masterAddress);

    // empty connect host and bind host with port
    conf.set(Constants.MASTER_HOSTNAME, "");
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("master.bind.host", 10000), masterAddress);

    // empty connect host and wildcard bind host with port
    conf.set(Constants.MASTER_BIND_HOST, Constants.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, 10000), masterAddress);
  }

  @Test
  public void testGetBindAddress() throws Exception {
    TachyonConf conf = new TachyonConf();
    String localHostName = NetworkAddressUtils.getLocalHostName(conf);
    int defaultPort = Constants.DEFAULT_WORKER_PORT;
    InetSocketAddress workerAddress;

    // all default
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(Constants.WILDCARD_ADDRESS, defaultPort),
        workerAddress);

    // bind host only
    conf.set(Constants.WORKER_BIND_HOST, "worker.bind.host");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("worker.bind.host", defaultPort), workerAddress);

    // connect host and bind host
    conf.set(Constants.WORKER_HOSTNAME, "worker.connect.host");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("worker.bind.host", defaultPort), workerAddress);

    // wildcard connect host and bind host
    conf.set(Constants.WORKER_HOSTNAME, Constants.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("worker.bind.host", defaultPort), workerAddress);

    // wildcard connect host and wildcard bind host
    conf.set(Constants.WORKER_BIND_HOST, Constants.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(Constants.WILDCARD_ADDRESS, defaultPort),
        workerAddress);

    // connect host and wildcard bind host
    conf.set(Constants.WORKER_HOSTNAME, "worker.connect.host");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(Constants.WILDCARD_ADDRESS, defaultPort),
        workerAddress);

    // connect host and wildcard bind host with port
    conf.set(Constants.WORKER_PORT, "20000");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(Constants.WILDCARD_ADDRESS, 20000), workerAddress);

    // connect host and bind host with port
    conf.set(Constants.WORKER_BIND_HOST, "worker.bind.host");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("worker.bind.host", 20000), workerAddress);

    // empty connect host and bind host with port
    conf.set(Constants.WORKER_HOSTNAME, "");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress("worker.bind.host", 20000), workerAddress);

    // empty connect host and wildcard bind host with port
    conf.set(Constants.WORKER_BIND_HOST, Constants.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(Constants.WILDCARD_ADDRESS, 20000), workerAddress);

    // empty connect host and empty bind host with port
    conf.set(Constants.WORKER_BIND_HOST, "");
    workerAddress = NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, 20000), workerAddress);

  }

  @Test
  public void replaceHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkAddressUtils.replaceHostName(TachyonURI.EMPTY_URI),
        TachyonURI.EMPTY_URI);
    Assert.assertEquals(NetworkAddressUtils.replaceHostName(null), null);

    TachyonURI[] paths =
        new TachyonURI[] {new TachyonURI("hdfs://localhost:9000/dir"),
            new TachyonURI("hdfs://localhost/dir"), new TachyonURI("hdfs://localhost/"),
            new TachyonURI("hdfs://localhost"), new TachyonURI("file:///dir"),
            new TachyonURI("/dir"), new TachyonURI("anythingElse")};

    for (TachyonURI path : paths) {
      Assert.assertEquals(NetworkAddressUtils.replaceHostName(path), path);
    }
  }

  @Test
  public void resolveHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkAddressUtils.resolveHostName(""), null);
    Assert.assertEquals(NetworkAddressUtils.resolveHostName(null), null);
    Assert.assertEquals(NetworkAddressUtils.resolveHostName("localhost"), "localhost");
  }

  @Test
  public void getFqdnHostTest() throws UnknownHostException {
    Assert.assertEquals(NetworkAddressUtils.getFqdnHost(new InetSocketAddress("localhost", 0)),
        "localhost");
    Assert.assertEquals(NetworkAddressUtils.getFqdnHost(new NetAddress("localhost", 0, 0)),
        "localhost");
  }
}
