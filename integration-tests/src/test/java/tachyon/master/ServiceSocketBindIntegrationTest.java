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

package tachyon.master;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.SocketChannel;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.client.block.BlockMasterClient;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.worker.BlockWorkerClient;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

/**
 * Simple integration tests for the bind configuration options.
 */
public class ServiceSocketBindIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(100, 100, Constants.GB, false);
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonConf mWorkerTachyonConf = null;
  private TachyonConf mMasterTachyonConf = null;

  private BlockMasterClient mBlockMasterClient;
  private HttpURLConnection mMasterWebService;
  private BlockWorkerClient mBlockWorkerClient;
  private SocketChannel mWorkerDataService;
  private HttpURLConnection mWorkerWebService;

  private void startCluster(String bindHost) throws Exception {
    for (ServiceType service : ServiceType.values()) {
      mLocalTachyonClusterResource.getTestConf().set(service.getBindHostKey(), bindHost);
    }
    mLocalTachyonClusterResource.start();
    mLocalTachyonCluster = mLocalTachyonClusterResource.get();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
  }

  private void connectServices() throws IOException, ConnectionFailedException {
    // connect Master RPC service
    mBlockMasterClient =
        new BlockMasterClient(new InetSocketAddress(mLocalTachyonCluster.getMasterHostname(),
            mLocalTachyonCluster.getMasterPort()), mMasterTachyonConf);
    mBlockMasterClient.connect();

    // connect Worker RPC service
    mBlockWorkerClient = BlockStoreContext.INSTANCE.acquireWorkerClient();
    mBlockWorkerClient.connect();

    // connect Worker data service
    mWorkerDataService = SocketChannel
        .open(NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_DATA, mWorkerTachyonConf));

    // connect Master Web service
    InetSocketAddress masterWebAddr =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB, mMasterTachyonConf);
    mMasterWebService =
        (HttpURLConnection) new URL("http://" + masterWebAddr.getAddress().getHostAddress() + ":"
            + masterWebAddr.getPort() + "/css/tachyoncustom.min.css").openConnection();
    mMasterWebService.connect();

    // connect Worker Web service
    InetSocketAddress workerWebAddr =
        NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_WEB, mWorkerTachyonConf);
    mWorkerWebService =
        (HttpURLConnection) new URL("http://" + workerWebAddr.getAddress().getHostAddress() + ":"
            + workerWebAddr.getPort() + "/css/tachyoncustom.min.css").openConnection();
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
    String bindHost = mLocalTachyonCluster.getMaster().getRPCBindHost();
    Assert.assertThat("Master RPC bind address " + bindHost + "is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Master RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker RPC socket bind (session layer)
    bindHost = mLocalTachyonCluster.getWorker().getRPCBindHost();
    Assert.assertThat("Worker RPC address " + bindHost + "is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Worker RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // TODO(andrew) Add this test back when we drop support for Java 6 and can use
    // InetSocketAddress.getHostString()
    // test Worker data socket bind (session layer)
    // bindHost = mLocalTachyonCluster.getWorker().getDataBindHost();
    // Assert.assertThat("Worker Data bind address " + bindHost + "is not wildcard address",
    // bindHost,
    // CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));

    // test Worker data service connectivity (application layer)
    Assert.assertTrue(mWorkerDataService.isConnected());

    // test Master Web socket bind (session layer)
    bindHost = mLocalTachyonCluster.getMaster().getWebBindHost();
    Assert.assertThat("Master Web bind address " + bindHost + "is not wildcard address", bindHost,
        CoreMatchers.containsString(NetworkAddressUtils.WILDCARD_ADDRESS));
    // test Master Web service connectivity (application layer)
    Assert.assertEquals(200, mMasterWebService.getResponseCode());

    // test Worker Web socket bind (session layer)
    bindHost = mLocalTachyonCluster.getWorker().getWebBindHost();
    Assert.assertThat("Worker Web bind address " + bindHost + "is not wildcard address", bindHost,
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
        new InetSocketAddress("127.0.0.1", mLocalTachyonCluster.getMaster().getRPCLocalPort());
    mBlockMasterClient = new BlockMasterClient(masterRPCAddr, mMasterTachyonConf);
    try {
      mBlockMasterClient.connect();
      Assert.fail("Client should not have successfully connected to master RPC service.");
    } catch (ConnectionFailedException e) {
      // This is expected, since Master RPC service is NOT listening on loopback.
    }

    // Connect to Worker RPC service on loopback, while Worker is listening on local hostname.
    try {
      mBlockWorkerClient = BlockStoreContext.INSTANCE.acquireWorkerClient("127.0.0.1");
      Assert.fail("Client should not have successfully connected to Worker RPC service.");
    } catch (RuntimeException rte) {
      // This is expected, since Work RPC service is NOT listening on loopback.
    }

    // connect Worker data service on loopback, while Worker is listening on local hostname.
    InetSocketAddress workerDataAddr =
        new InetSocketAddress("127.0.0.1", mLocalTachyonCluster.getWorker().getDataLocalPort());
    try {
      mWorkerDataService = SocketChannel.open(workerDataAddr);
      Assert.assertTrue(mWorkerDataService.isConnected());
      Assert.fail("Client should not have successfully connected to Worker RPC service.");
    } catch (IOException ie) {
      // This is expected, since Worker data service is NOT listening on loopback.
    }

    // connect Master Web service on loopback, while Master is listening on local hostname.
    try {
      mMasterWebService = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + mLocalTachyonCluster.getMaster().getWebLocalPort() + "/home")
          .openConnection();
      Assert.assertEquals(200, mMasterWebService.getResponseCode());
      Assert.fail("Client should not have successfully connected to Master Web service.");
    } catch (IOException ie) {
      // This is expected, since Master Web service is NOT listening on loopback.
    } finally {
      Assert.assertNotNull(mMasterWebService);
      mMasterWebService.disconnect();
    }

    // connect Worker Web service on loopback, while Worker is listening on local hostname.
    try {
      mWorkerWebService = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + mLocalTachyonCluster.getWorker().getWebLocalPort() + "/home")
              .openConnection();
      Assert.assertEquals(200, mWorkerWebService.getResponseCode());
      Assert.fail("Client should not have successfully connected to Worker Web service.");
    } catch (IOException ie) {
      // This is expected, since Worker Web service is NOT listening on loopback.
    } finally {
      mWorkerWebService.disconnect();
    }
  }
}
