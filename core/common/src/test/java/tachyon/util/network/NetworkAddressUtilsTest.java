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
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.wire.WorkerNetAddress;

/**
 * Tests for the {@link NetworkAddressUtils} class.
 */
public class NetworkAddressUtilsTest {

  /**
   * Tests the {@link NetworkAddressUtils#getConnectAddress(ServiceType, TachyonConf)} method.
   *
   * @throws Exception thrown if something goes wrong
   */
  @Test
  public void testGetConnectAddress() throws Exception {
    for (ServiceType service : ServiceType.values()) {
      getConnectAddress(service);
    }
  }

  private void getConnectAddress(ServiceType service) throws Exception {
    TachyonConf conf = new TachyonConf();
    String localHostName = NetworkAddressUtils.getLocalHostName(conf);
    InetSocketAddress masterAddress;

    // all default
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, service.getDefaultPort()),
        masterAddress);

    // bind host only
    conf.set(service.getHostNameKey(), "");
    conf.set(service.getBindHostKey(), "bind.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert
        .assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), masterAddress);

    // connect host and bind host
    conf.set(service.getHostNameKey(), "connect.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("connect.host", service.getDefaultPort()),
        masterAddress);

    // wildcard connect host and bind host
    conf.set(service.getHostNameKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert
        .assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), masterAddress);

    // wildcard connect host and wildcard bind host
    conf.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, service.getDefaultPort()),
        masterAddress);

    // connect host and wildcard bind host
    conf.set(service.getHostNameKey(), "connect.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("connect.host", service.getDefaultPort()),
        masterAddress);

    // connect host and wildcard bind host with port
    conf.set(service.getPortKey(), "10000");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("connect.host", 10000), masterAddress);

    // connect host and bind host with port
    conf.set(service.getBindHostKey(), "bind.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("connect.host", 10000), masterAddress);

    // empty connect host and bind host with port
    conf.set(service.getHostNameKey(), "");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("bind.host", 10000), masterAddress);

    // empty connect host and wildcard bind host with port
    conf.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, 10000), masterAddress);
  }

  /**
   * Tests the {@link NetworkAddressUtils#getBindAddress(ServiceType, TachyonConf)} method.
   *
   * @throws Exception thrown if something goes wrong
   */
  @Test
  public void testGetBindAddress() throws Exception {
    for (ServiceType service : ServiceType.values()) {
      getBindAddress(service);
    }
  }

  private void getBindAddress(ServiceType service) throws Exception {
    TachyonConf conf = new TachyonConf();
    String localHostName = NetworkAddressUtils.getLocalHostName(conf);
    InetSocketAddress workerAddress;

    // all default
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(
        new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, service.getDefaultPort()),
        workerAddress);

    // bind host only
    conf.set(service.getBindHostKey(), "bind.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert
        .assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), workerAddress);

    // connect host and bind host
    conf.set(service.getHostNameKey(), "connect.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert
        .assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), workerAddress);

    // wildcard connect host and bind host
    conf.set(service.getHostNameKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert
        .assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), workerAddress);

    // wildcard connect host and wildcard bind host
    conf.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(
        new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, service.getDefaultPort()),
        workerAddress);

    // connect host and wildcard bind host
    conf.set(service.getHostNameKey(), "connect.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(
        new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, service.getDefaultPort()),
        workerAddress);

    // connect host and wildcard bind host with port
    switch (service) {
      case MASTER_RPC:
        conf.set(Constants.MASTER_RPC_PORT, "20000");
        break;
      case MASTER_WEB:
        conf.set(Constants.MASTER_WEB_PORT, "20000");
        break;
      case WORKER_RPC:
        conf.set(Constants.WORKER_RPC_PORT, "20000");
        break;
      case WORKER_DATA:
        conf.set(Constants.WORKER_DATA_PORT, "20000");
        break;
      case WORKER_WEB:
        conf.set(Constants.WORKER_WEB_PORT, "20000");
        break;
      default:
        Assert.fail("Unrecognized service type: " + service.toString());
        break;
    }
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, 20000),
        workerAddress);

    // connect host and bind host with port
    conf.set(service.getBindHostKey(), "bind.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("bind.host", 20000), workerAddress);

    // empty connect host and bind host with port
    conf.set(service.getHostNameKey(), "");
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress("bind.host", 20000), workerAddress);

    // empty connect host and wildcard bind host with port
    conf.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, 20000),
        workerAddress);

    // empty connect host and empty bind host with port
    conf.set(service.getBindHostKey(), "");
    workerAddress = NetworkAddressUtils.getBindAddress(service, conf);
    Assert.assertEquals(new InetSocketAddress(localHostName, 20000), workerAddress);
  }

  /**
   * Tests the {@link NetworkAddressUtils#replaceHostName(TachyonURI)} method.
   *
   * @throws UnknownHostException thrown if the host is unknown
   */
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

  /**
   * Tests the {@link NetworkAddressUtils#resolveHostName(String)} method.
   *
   * @throws UnknownHostException thrown if the host is unknown
   */
  @Test
  public void resolveHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkAddressUtils.resolveHostName(""), null);
    Assert.assertEquals(NetworkAddressUtils.resolveHostName(null), null);
    Assert.assertEquals(NetworkAddressUtils.resolveHostName("localhost"), "localhost");
  }

  /**
   * Tests the {@link NetworkAddressUtils#getFqdnHost(InetSocketAddress)} and
   * {@link NetworkAddressUtils#getFqdnHost(WorkerNetAddress)} methods.
   *
   * @throws UnknownHostException thrown if the host is unknown
   */
  @Test
  public void getFqdnHostTest() throws UnknownHostException {
    Assert.assertEquals(NetworkAddressUtils.getFqdnHost(new InetSocketAddress("localhost", 0)),
        "localhost");
    Assert.assertEquals(NetworkAddressUtils.getFqdnHost(new WorkerNetAddress("localhost", 0, 0, 0)),
        "localhost");
  }
}
