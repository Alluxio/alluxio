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

package alluxio.web;

import alluxio.ClientContext;
import alluxio.client.block.BlockMasterClient;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientContext;
import alluxio.master.SingleMasterInquireClient;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
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
public class ServiceSocketBindIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();
  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private BlockMasterClient mBlockMasterClient;
  private HttpURLConnection mMasterWebService;
  private SocketChannel mWorkerDataService;
  private HttpURLConnection mWorkerWebService;

  /**
   * Starts the {@link LocalAlluxioCluster}.
   *
   * @param bindHost the local host name to bind
   */
  private void startCluster(String bindHost) throws Exception {
    for (ServiceType service : ServiceType.values()) {
      mLocalAlluxioClusterResource.setProperty(service.getBindHostKey(), bindHost);
    }
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
  }

  /**
   * Connect different services in turn, including Master RPC, Worker RPC, Worker data, Master Web,
   * and Worker Web service.
   */
  private void connectServices() throws IOException, ConnectionFailedException {
    // connect Master RPC service
    mBlockMasterClient =
        BlockMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    mBlockMasterClient.connect();

    // connect Worker RPC service
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();

    // connect Worker data service
    mWorkerDataService = SocketChannel
        .open(new InetSocketAddress(workerAddress.getHost(), workerAddress.getDataPort()));

    // connect Master Web service
    InetSocketAddress masterWebAddr =
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB, ServerConfiguration.global());
    mMasterWebService = (HttpURLConnection) new URL("http://"
        + (masterWebAddr.isUnresolved() ? masterWebAddr.toString()
            : masterWebAddr.getAddress().getHostAddress() + ":" + masterWebAddr.getPort())
        + "/index.html").openConnection();
    mMasterWebService.connect();

    // connect Worker Web service
    InetSocketAddress workerWebAddr =
        new InetSocketAddress(workerAddress.getHost(), workerAddress.getWebPort());
    mWorkerWebService = (HttpURLConnection) new URL(
        "http://" + workerWebAddr.getAddress().getHostAddress() + ":" + workerWebAddr.getPort()
            + "/index.html").openConnection();
    mWorkerWebService.connect();
  }

  private void closeServices() throws Exception {
    mWorkerWebService.disconnect();
    mWorkerDataService.close();
    mMasterWebService.disconnect();
    mBlockMasterClient.close();
  }

  @Test
  public void listenEmpty() throws Exception {
    startCluster(null);
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
    startCluster(NetworkAddressUtils.getLocalHostName(100));

    // Connect to Master RPC service on loopback, while Master is listening on local hostname.
    InetSocketAddress masterRpcAddr = new InetSocketAddress("127.0.0.1",
        mLocalAlluxioCluster.getLocalAlluxioMaster().getRpcLocalPort());
    mBlockMasterClient = BlockMasterClient.Factory.create(MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global()))
        .setMasterInquireClient(new SingleMasterInquireClient(masterRpcAddr))
        .build());
    try {
      mBlockMasterClient.connect();
      Assert.fail("Client should not have successfully connected to master RPC service.");
    } catch (UnavailableException e) {
      // This is expected, since Master RPC service is NOT listening on loopback.
    }

    // Connect to Worker RPC service on loopback, while Worker is listening on local hostname.
    try {
      mBlockMasterClient.connect();
      Assert.fail("Client should not have successfully connected to Worker RPC service.");
    } catch (Exception e) {
      // This is expected, since Work RPC service is NOT listening on loopback.
    }

    // connect Worker data service on loopback, while Worker is listening on local hostname.
    InetSocketAddress workerDataAddr = new InetSocketAddress("127.0.0.1",
        mLocalAlluxioCluster.getWorkerProcess().getDataLocalPort());
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
          "http://127.0.0.1:" + mLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess()
              .getWebAddress().getPort() + "/home").openConnection();
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
          "http://127.0.0.1:" + mLocalAlluxioCluster.getWorkerProcess().getWebLocalPort() + "/home")
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
