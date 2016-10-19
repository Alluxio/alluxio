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

import alluxio.LocalAlluxioClusterResource;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.exception.ConnectionFailedException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerNetAddress;

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
      new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();
  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private BlockMasterClient mBlockMasterClient;
  private HttpURLConnection mMasterWebService;
  private BlockWorkerClient mBlockWorkerClient;
  private SocketChannel mWorkerDataService;
  private HttpURLConnection mWorkerWebService;

  private void startCluster(String bindHost) throws Exception {
    for (ServiceType service : ServiceType.values()) {
      mLocalAlluxioClusterResource.setProperty(service.getBindHostKey(), bindHost);
    }
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
  }

  private void connectServices() throws IOException, ConnectionFailedException {
    // connect Master RPC service
    mBlockMasterClient = new RetryHandlingBlockMasterClient(
        new InetSocketAddress(mLocalAlluxioCluster.getHostname(),
            mLocalAlluxioCluster.getMasterRpcPort()));
    mBlockMasterClient.connect();

    // connect Worker RPC service
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    mBlockWorkerClient = BlockStoreContext.get().createWorkerClient(workerAddress);

    // connect Worker data service
    mWorkerDataService = SocketChannel
        .open(new InetSocketAddress(workerAddress.getHost(), workerAddress.getDataPort()));

    // connect Master Web service
    InetSocketAddress masterWebAddr =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB);
    mMasterWebService =
        (HttpURLConnection) new URL("http://" + masterWebAddr.getAddress().getHostAddress() + ":"
            + masterWebAddr.getPort() + "/css/custom.min.css").openConnection();
    mMasterWebService.connect();

    // connect Worker Web service
    InetSocketAddress workerWebAddr =
        new InetSocketAddress(workerAddress.getHost(), workerAddress.getWebPort());
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
  public void listenEmpty() throws Exception {
    startCluster("");
    boolean allConnected = true;
    try {
      connectServices();
    } catch (Exception e) {
      allConnected = false;
    }

    Assert.assertTrue(allConnected);

    // test Master RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker data service connectivity (application layer)
    Assert.assertTrue(mWorkerDataService.isConnected());

    // test Master Web service connectivity (application layer)
    Assert.assertEquals(200, mMasterWebService.getResponseCode());

    // test Worker Web service connectivity (application layer)
    Assert.assertEquals(200, mWorkerWebService.getResponseCode());

    closeServices();
  }

  @Test
  public void listenSameAddress() throws Exception {
    startCluster(NetworkAddressUtils.getLocalHostName(100));
    boolean allConnected = true;
    try {
      connectServices();
    } catch (Exception e) {
      allConnected = false;
    }

    Assert.assertTrue(allConnected);

    // test Master RPC service connectivity (application layer)
    Assert.assertTrue(mBlockMasterClient.isConnected());

    // test Worker data service connectivity (application layer)
    Assert.assertTrue(mWorkerDataService.isConnected());

    // test Master Web service connectivity (application layer)
    Assert.assertEquals(200, mMasterWebService.getResponseCode());

    // test Worker Web service connectivity (application layer)
    Assert.assertEquals(200, mWorkerWebService.getResponseCode());

    closeServices();
  }

  @Test
  public void connectDifferentAddress() throws Exception {
    startCluster("");

    // Connect to Master RPC service on loopback, while Master is listening on local hostname.
    InetSocketAddress masterRPCAddr =
        new InetSocketAddress("127.0.0.1", mLocalAlluxioCluster.getMaster().getRPCLocalPort());
    mBlockMasterClient = new RetryHandlingBlockMasterClient(masterRPCAddr);
    try {
      mBlockMasterClient.connect();
      Assert.fail("Client should not have successfully connected to master RPC service.");
    } catch (ConnectionFailedException e) {
      // This is expected, since Master RPC service is NOT listening on loopback.
    }

    // Connect to Worker RPC service on loopback, while Worker is listening on local hostname.
    try {
      mBlockWorkerClient =
          BlockStoreContext.get().createWorkerClient(mLocalAlluxioCluster.getWorkerAddress());
      mBlockMasterClient.connect();
      Assert.fail("Client should not have successfully connected to Worker RPC service.");
    } catch (Exception e) {
      // This is expected, since Work RPC service is NOT listening on loopback.
    } finally {
      mBlockWorkerClient.close();
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
