/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.exception.ConnectionFailedException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.SocketChannel;

/**
 * Simple integration tests for the bind configuration options.
 */
public class ServiceSocketBindIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(100, Constants.GB, false);
  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private Configuration mWorkerConfiguration = null;
  private Configuration mMasterConfiguration = null;

  private BlockMasterClient mBlockMasterClient;
  private HttpURLConnection mMasterWebService;
  private BlockWorkerClient mBlockWorkerClient;
  private SocketChannel mWorkerDataService;
  private HttpURLConnection mWorkerWebService;

  private void startCluster(String bindHost) throws Exception {
    for (ServiceType service : ServiceType.values()) {
      mLocalAlluxioClusterResource.getTestConf().set(service.getBindHostKey(), bindHost);
    }
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mMasterConfiguration = mLocalAlluxioCluster.getMasterConf();
    mWorkerConfiguration = mLocalAlluxioCluster.getWorkerConf();
  }

  private void connectServices() throws IOException, ConnectionFailedException {
    // connect Master RPC service
    mBlockMasterClient =
        new BlockMasterClient(new InetSocketAddress(mLocalAlluxioCluster.getMasterHostname(),
            mLocalAlluxioCluster.getMasterPort()), mMasterConfiguration);
    mBlockMasterClient.connect();

    // connect Worker RPC service
    mBlockWorkerClient = BlockStoreContext.INSTANCE.acquireLocalWorkerClient();
    mBlockWorkerClient.connect();

    // connect Worker data service
    mWorkerDataService = SocketChannel
        .open(NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_DATA, mWorkerConfiguration));

    // connect Master Web service
    InetSocketAddress masterWebAddr =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB, mMasterConfiguration);
    mMasterWebService =
        (HttpURLConnection) new URL("http://" + masterWebAddr.getAddress().getHostAddress() + ":"
            + masterWebAddr.getPort() + "/css/custom.min.css").openConnection();
    mMasterWebService.connect();

    // connect Worker Web service
    InetSocketAddress workerWebAddr =
        NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_WEB, mWorkerConfiguration);
    mWorkerWebService =
        (HttpURLConnection) new URL("http://" + workerWebAddr.getAddress().getHostAddress() + ":"
            + workerWebAddr.getPort() + "/css/custom.min.css").openConnection();
    mWorkerWebService.connect();
  }

  private void closeServices() throws Exception {
    mWorkerWebService.disconnect();
    mWorkerDataService.close();
    mBlockWorkerClient.close();
    mMasterWebService.disconnect();
    mBlockMasterClient.close();
  }

  @Test
  public void listenEmptyTest() throws Exception {
    startCluster("");
    connectServices();

    // test Master RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker RPC service connectivity (application layer)
    Assert.assertTrue(mBlockWorkerClient.isConnected());

    // test Worker data service connectivity (application layer)
    Assert.assertTrue(mWorkerDataService.isConnected());

    // test Master Web service connectivity (application layer)
    Assert.assertEquals(200, mMasterWebService.getResponseCode());

    // test Worker Web service connectivity (application layer)
    Assert.assertEquals(200, mWorkerWebService.getResponseCode());

    closeServices();
  }

  @Test
  public void listenWildcardTest() throws Exception {
    startCluster(NetworkAddressUtils.WILDCARD_ADDRESS);
    connectServices();

    // test Master RPC socket bind (session layer)
    String bindHost = mLocalAlluxioCluster.getMaster().getRPCBindHost();
    Assert.assertThat("Master RPC bind address " + bindHost + " is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Master RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker RPC socket bind (session layer)
    bindHost = mLocalAlluxioCluster.getWorker().getRPCBindHost();
    Assert.assertThat("Worker RPC address " + bindHost + " is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Worker RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker data socket bind (session layer)
    bindHost = mLocalAlluxioCluster.getWorker().getDataBindHost();
    Assert.assertThat(
        "Worker Data bind address " + bindHost + " is not wildcard address. Make sure the System"
            + " property -Djava.net.preferIPv4Stack is set to true.",
        bindHost, CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));

    // test Worker data service connectivity (application layer)
    Assert.assertTrue(mWorkerDataService.isConnected());

    // test Master Web socket bind (session layer)
    bindHost = mLocalAlluxioCluster.getMaster().getWebBindHost();
    Assert.assertThat("Master Web bind address " + bindHost + " is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Master Web service connectivity (application layer)
    Assert.assertEquals(200, mMasterWebService.getResponseCode());

    // test Worker Web socket bind (session layer)
    bindHost = mLocalAlluxioCluster.getWorker().getWebBindHost();
    Assert.assertThat("Worker Web bind address " + bindHost + " is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Worker Web service connectivity (application layer)
    Assert.assertEquals(200, mWorkerWebService.getResponseCode());

    closeServices();
  }

  @Test
  public void listenSameAddressTest() throws Exception {
    startCluster(NetworkAddressUtils.getLocalHostName(100));
    connectServices();

    // test Master RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker RPC service connectivity (application layer)
    Assert.assertTrue(mBlockWorkerClient.isConnected());

    // test Worker data service connectivity (application layer)
    Assert.assertTrue(mWorkerDataService.isConnected());

    // test Master Web service connectivity (application layer)
    Assert.assertEquals(200, mMasterWebService.getResponseCode());

    // test Worker Web service connectivity (application layer)
    Assert.assertEquals(200, mWorkerWebService.getResponseCode());

    closeServices();
  }

  @Test
  public void connectDifferentAddressTest() throws Exception {
    startCluster("");

    // Connect to Master RPC service on loopback, while Master is listening on local hostname.
    InetSocketAddress masterRPCAddr =
        new InetSocketAddress("127.0.0.1", mLocalAlluxioCluster.getMaster().getRPCLocalPort());
    mBlockMasterClient = new BlockMasterClient(masterRPCAddr, mMasterConfiguration);
    try {
      mBlockMasterClient.connect();
      Assert.fail("Client should not have successfully connected to master RPC service.");
    } catch (ConnectionFailedException e) {
      // This is expected, since Master RPC service is NOT listening on loopback.
    }

    // Connect to Worker RPC service on loopback, while Worker is listening on local hostname.
    try {
      mBlockWorkerClient = BlockStoreContext.INSTANCE
          .acquireWorkerClient(mLocalAlluxioCluster.getWorker().getNetAddress());
      mBlockMasterClient.connect();
    } catch (Exception e) {
      // This is expected, since Work RPC service is NOT listening on loopback.
    }

    // connect Worker data service on loopback, while Worker is listening on local hostname.
    InetSocketAddress workerDataAddr =
        new InetSocketAddress("127.0.0.1", mLocalAlluxioCluster.getWorker().getDataLocalPort());
    try {
      mWorkerDataService = SocketChannel.open(workerDataAddr);
      Assert.assertTrue(mWorkerDataService.isConnected());
      Assert.fail("Client should not have successfully connected to Worker RPC service.");
    } catch (IOException e) {
      // This is expected, since Worker data service is NOT listening on loopback.
    }

    // connect Master Web service on loopback, while Master is listening on local hostname.
    try {
      mMasterWebService = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + mLocalAlluxioCluster.getMaster().getWebLocalPort() + "/home")
          .openConnection();
      Assert.assertEquals(200, mMasterWebService.getResponseCode());
      Assert.fail("Client should not have successfully connected to Master Web service.");
    } catch (IOException e) {
      // This is expected, since Master Web service is NOT listening on loopback.
    } finally {
      Assert.assertNotNull(mMasterWebService);
      mMasterWebService.disconnect();
    }

    // connect Worker Web service on loopback, while Worker is listening on local hostname.
    try {
      mWorkerWebService = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + mLocalAlluxioCluster.getWorker().getWebLocalPort() + "/home")
              .openConnection();
      Assert.assertEquals(200, mWorkerWebService.getResponseCode());
      Assert.fail("Client should not have successfully connected to Worker Web service.");
    } catch (IOException e) {
      // This is expected, since Worker Web service is NOT listening on loopback.
    } finally {
      mWorkerWebService.disconnect();
    }
  }
}
