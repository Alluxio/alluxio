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

import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.CommonUtils.ProcessType;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Tests for the {@link NetworkAddressUtils} class.
 */
public class NetworkAddressUtilsTest {

  private InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  @After
  public void after() {
    mConfiguration = ConfigurationTestUtils.defaults();
  }

  /**
   * Tests the
   * {@link NetworkAddressUtils#getConnectAddress(ServiceType, alluxio.conf.AlluxioConfiguration)}
   * method.
   */
  @Test
  public void testGetConnectAddress() throws Exception {
    for (ServiceType service : ServiceType.values()) {
      getConnectAddress(service);
    }
  }

  /**
   * Tests the
   * {@link NetworkAddressUtils#getConnectAddress(ServiceType, alluxio.conf.AlluxioConfiguration)}
   * method
   * for specific
   * service under different conditions.
   *
   * @param service the service name used to connect
   */
  private void getConnectAddress(ServiceType service) throws Exception {
    int resolveTimeout = (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    String localHostName = NetworkAddressUtils.getLocalHostName(resolveTimeout);
    InetSocketAddress masterAddress;

    // all default
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress(localHostName, service.getDefaultPort()),
        masterAddress);

    // bind host only
    mConfiguration.unset(service.getHostNameKey());
    mConfiguration.set(service.getBindHostKey(), "bind.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), masterAddress);

    // connect host and bind host
    mConfiguration.set(service.getHostNameKey(), "connect.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("connect.host", service.getDefaultPort()),
        masterAddress);

    // wildcard connect host and bind host
    mConfiguration.set(service.getHostNameKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), masterAddress);

    // wildcard connect host and wildcard bind host
    mConfiguration.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress(localHostName, service.getDefaultPort()),
        masterAddress);

    // connect host and wildcard bind host
    mConfiguration.set(service.getHostNameKey(), "connect.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("connect.host", service.getDefaultPort()),
        masterAddress);

    // connect host and wildcard bind host with port
    mConfiguration.set(service.getPortKey(), "10000");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("connect.host", 10000), masterAddress);

    // connect host and bind host with port
    mConfiguration.set(service.getBindHostKey(), "bind.host");
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("connect.host", 10000), masterAddress);

    // empty connect host and bind host with port
    mConfiguration.unset(service.getHostNameKey());
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", 10000), masterAddress);

    // empty connect host and wildcard bind host with port
    mConfiguration.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    masterAddress = NetworkAddressUtils.getConnectAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress(localHostName, 10000), masterAddress);
  }

   /**
    * Tests the
    * {@link NetworkAddressUtils#getBindAddress(ServiceType, alluxio.conf.AlluxioConfiguration)}
    * method.
   */
  @Test
  public void testGetBindAddress() throws Exception {
    for (ServiceType service : ServiceType.values()) {
      getBindAddress(service);
    }
  }

  /**
   * Tests the {@link NetworkAddressUtils#getBindAddress(ServiceType,
   * alluxio.conf.AlluxioConfiguration)} method for specific service under different conditions.
   *
   * @param service the service name used to connect
   */
  private void getBindAddress(ServiceType service) throws Exception {
    int resolveTimeout = (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    String localHostName = NetworkAddressUtils.getLocalHostName(resolveTimeout);
    InetSocketAddress workerAddress;

    // all default
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(
        new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, service.getDefaultPort()),
        workerAddress);

    // bind host only
    mConfiguration.set(service.getBindHostKey(), "bind.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), workerAddress);

    // connect host and bind host
    mConfiguration.set(service.getHostNameKey(), "connect.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), workerAddress);

    // wildcard connect host and bind host
    mConfiguration.set(service.getHostNameKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", service.getDefaultPort()), workerAddress);

    // wildcard connect host and wildcard bind host
    mConfiguration.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(
        new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, service.getDefaultPort()),
        workerAddress);

    // connect host and wildcard bind host
    mConfiguration.set(service.getHostNameKey(), "connect.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(
        new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, service.getDefaultPort()),
        workerAddress);

    // connect host and wildcard bind host with port
    switch (service) {
      case JOB_MASTER_RAFT:
        mConfiguration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, "20000");
        break;
      case MASTER_RAFT:
        mConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, "20000");
        break;
      case JOB_MASTER_RPC:
        mConfiguration.set(PropertyKey.JOB_MASTER_RPC_PORT, "20000");
        break;
      case JOB_MASTER_WEB:
        mConfiguration.set(PropertyKey.JOB_MASTER_WEB_PORT, "20000");
        break;
      case JOB_WORKER_RPC:
        mConfiguration.set(PropertyKey.JOB_WORKER_RPC_PORT, "20000");
        break;
      case JOB_WORKER_WEB:
        mConfiguration.set(PropertyKey.JOB_WORKER_WEB_PORT, "20000");
        break;
      case MASTER_RPC:
        mConfiguration.set(PropertyKey.MASTER_RPC_PORT, "20000");
        break;
      case MASTER_WEB:
        mConfiguration.set(PropertyKey.MASTER_WEB_PORT, "20000");
        break;
      case PROXY_WEB:
        mConfiguration.set(PropertyKey.PROXY_WEB_PORT, "20000");
        break;
      case WORKER_RPC:
        mConfiguration.set(PropertyKey.WORKER_RPC_PORT, "20000");
        break;
      case WORKER_WEB:
        mConfiguration.set(PropertyKey.WORKER_WEB_PORT, "20000");
        break;
      default:
        Assert.fail("Unrecognized service type: " + service.toString());
        break;
    }
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, 20000),
        workerAddress);

    // connect host and bind host with port
    mConfiguration.set(service.getBindHostKey(), "bind.host");
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", 20000), workerAddress);

    // empty connect host and bind host with port
    mConfiguration.unset(service.getHostNameKey());
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress("bind.host", 20000), workerAddress);

    // empty connect host and wildcard bind host with port
    mConfiguration.set(service.getBindHostKey(), NetworkAddressUtils.WILDCARD_ADDRESS);
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, 20000),
        workerAddress);

    // unset connect host and bind host with port
    mConfiguration.unset(service.getBindHostKey());
    workerAddress = NetworkAddressUtils.getBindAddress(service, mConfiguration);
    assertEquals(new InetSocketAddress(NetworkAddressUtils.WILDCARD_ADDRESS, 20000), workerAddress);
  }

  @Test
  public void getLocalNodeNameClient() throws Exception {
    CommonUtils.PROCESS_TYPE.set(ProcessType.CLIENT);
    try (Closeable c = new ConfigurationRule(PropertyKey.USER_HOSTNAME, "client", mConfiguration)
        .toResource()) {
      assertEquals("client", NetworkAddressUtils.getLocalNodeName(mConfiguration));
    }
  }

  @Test
  public void getLocalNodeNameWorker() throws Exception {
    CommonUtils.PROCESS_TYPE.set(ProcessType.WORKER);
    try (Closeable c = new ConfigurationRule(PropertyKey.WORKER_HOSTNAME, "worker", mConfiguration)
        .toResource()) {
      assertEquals("worker", NetworkAddressUtils.getLocalNodeName(mConfiguration));
    }
  }

  @Test
  public void getLocalNodeNameMaster() throws Exception {
    CommonUtils.PROCESS_TYPE.set(ProcessType.MASTER);
    try (Closeable c = new ConfigurationRule(PropertyKey.MASTER_HOSTNAME, "master", mConfiguration)
        .toResource()) {
      assertEquals("master", NetworkAddressUtils.getLocalNodeName(mConfiguration));
    }
  }

  @Test
  public void getLocalNodeNameLookup() throws Exception {
    int resolveTimeout = (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    assertEquals(NetworkAddressUtils.getLocalHostName(resolveTimeout),
        NetworkAddressUtils.getLocalNodeName(mConfiguration));
  }

  /**
   * Tests the {@link NetworkAddressUtils#resolveHostName(String)} method.
   */
  @Test
  public void resolveHostName() throws UnknownHostException {
    assertEquals(NetworkAddressUtils.resolveHostName(""), null);
    assertEquals(NetworkAddressUtils.resolveHostName(null), null);
    assertEquals(NetworkAddressUtils.resolveHostName("localhost"), "localhost");
  }

  /**
   * Tests the {@link NetworkAddressUtils#resolveIpAddress(String)} method.
   */
  @Test
  public void resolveIpAddress() throws UnknownHostException {
    assertEquals(NetworkAddressUtils.resolveIpAddress("localhost"), "127.0.0.1");
    assertEquals(NetworkAddressUtils.resolveIpAddress("127.0.0.1"), "127.0.0.1");
  }

  /**
   * Tests the {@link NetworkAddressUtils#resolveIpAddress(String)} method.
   */
  @Test(expected = NullPointerException.class)
  public void resolveNullIpAddress() throws UnknownHostException {
    NetworkAddressUtils.resolveIpAddress(null);
  }

  /**
   * Tests the {@link NetworkAddressUtils#resolveIpAddress(String)} method.
   */
  @Test(expected = IllegalArgumentException.class)
  public void resolveEmptyIpAddress() throws UnknownHostException {
    NetworkAddressUtils.resolveIpAddress("");
  }

  /**
   * Tests the {@link NetworkAddressUtils#getFqdnHost(InetSocketAddress)} and
   * {@link NetworkAddressUtils#getFqdnHost(WorkerNetAddress)} methods.
   */
  @Test
  public void getFqdnHost() throws UnknownHostException {
    assertEquals(NetworkAddressUtils.getFqdnHost(new InetSocketAddress("localhost", 0)),
        "localhost");
    assertEquals(
        NetworkAddressUtils.getFqdnHost(new WorkerNetAddress().setHost("localhost")), "localhost");
  }

  @Test
  public void getConfiguredClientHostname() {
    mConfiguration.set(PropertyKey.USER_HOSTNAME, "clienthost");
    assertEquals("clienthost", NetworkAddressUtils.getClientHostName(mConfiguration));
  }

  @Test
  public void getDefaultClientHostname() {
    int resolveTimeout = (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    assertEquals(NetworkAddressUtils.getLocalHostName(resolveTimeout),
        NetworkAddressUtils.getClientHostName(mConfiguration));
  }
}
