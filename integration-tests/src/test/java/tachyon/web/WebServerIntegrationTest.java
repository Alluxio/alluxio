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

package tachyon.web;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Scanner;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.BlockMasterClient;
import tachyon.client.block.BlockStoreContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterContext;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.WorkerClient;

/**
 * Test the web server is up when Tachyon starts.
 *
 * <p>
 */
public class WebServerIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;
  private TachyonConf mWorkerTachyonConf = null;

  private boolean verifyWebService(HttpURLConnection webService) throws IOException {
    if (null == webService) {
      return false;
    }

    Scanner pageScanner = new Scanner(webService.getInputStream());
    boolean verified = false;

    while (pageScanner.hasNextLine()) {
      String line = pageScanner.nextLine();
      if (line.equals("<title>Tachyon</title>")) {
        verified = true;
        break;
      }
    }
    pageScanner.close();
    return verified;
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
  }

  @Before
  public final void before() throws Exception {
    TachyonConf tachyonConf = MasterContext.getConf();
    for (ServiceType service : ServiceType.values()) {
      tachyonConf.set(service.getBindHostKey(),
          NetworkAddressUtils.getLocalHostName(100));
    }
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
  }

  @Test
  public void serverUpTest() throws Exception, IOException, TachyonException {
    BlockMasterClient blockMasterClient =
        new BlockMasterClient(new InetSocketAddress(mLocalTachyonCluster.getMasterHostname(),
            mLocalTachyonCluster.getMasterPort()), mExecutorService, mMasterTachyonConf);
    blockMasterClient.connect();
    Assert.assertTrue(blockMasterClient.isConnected());

    WorkerClient workerClient = BlockStoreContext.INSTANCE.acquireWorkerClient();
    workerClient.mustConnect();
    Assert.assertTrue(workerClient.isConnected());

    SocketChannel workerDataService = SocketChannel
        .open(NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_DATA, mWorkerTachyonConf));
    Assert.assertTrue(workerDataService.isConnected());

    try {
      HttpURLConnection masterWebService = (HttpURLConnection) new URL(
          "http://" + mLocalTachyonCluster.getMaster().getWebBindHost() + ":"
          + mLocalTachyonCluster.getMaster().getWebLocalPort() + "/home")
          .openConnection();
      Assert.assertNotNull(masterWebService);
      masterWebService.connect();
      Assert.assertEquals(200, masterWebService.getResponseCode());
      if (!verifyWebService(masterWebService)) {
        Assert.fail("Master web server was started but not successfully verified.");
      }
      masterWebService.disconnect();
    } catch (IOException e) {
      Assert.fail("Master web server was not successfully started.");
    }

    try {
      HttpURLConnection workerWebService = (HttpURLConnection) new URL(
          "http://" + mLocalTachyonCluster.getWorker().getWebBindHost() + ":"
          + mLocalTachyonCluster.getWorker().getWebLocalPort() + "/home")
          .openConnection();
      Assert.assertNotNull(workerWebService);
      workerWebService.connect();
      Assert.assertEquals(200, workerWebService.getResponseCode());
      if (!verifyWebService(workerWebService)) {
        Assert.fail("Worker web server was started but not successfully verified.");
      }
      workerWebService.disconnect();
    } catch (IOException e) {
      Assert.fail("Worker web server was not successfully started.");
    }
  }
}
