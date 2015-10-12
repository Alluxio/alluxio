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
import java.util.Scanner;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.FileSystemMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.exception.TachyonException;

/**
 * Test the web server is up when Tachyon starts.
 *
 * <p>
 */
public class WebServerIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonConf mMasterTachyonConf = null;
  private TachyonConf mWorkerTachyonConf = null;

  private HttpURLConnection mMasterWebService;
  private HttpURLConnection mWorkerWebService;

  private boolean verifyWebService(HttpURLConnection webService) throws IOException {
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
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
  }

  @Test
  public void serverUpTest() throws TachyonException, IOException {
    try {
      InetSocketAddress masterWebAddr =
          NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB, mMasterTachyonConf);
      HttpURLConnection mMasterWebService = (HttpURLConnection) new URL(
          "http://" + masterWebAddr.getAddress().getHostAddress() + ":"
          + mLocalTachyonCluster.getMaster().getWebLocalPort() + "/home")
          .openConnection();
      Assert.assertEquals(200, mMasterWebService.getResponseCode());
      if (!verifyWebService(mMasterWebService)) {
        Assert.fail("Master web server was started but not successfully verified.");
      }
    } catch (IOException e) {
      Assert.fail("Master web server was not successfully started.");
    }

    try {
      InetSocketAddress workerWebAddr =
          NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_WEB, mWorkerTachyonConf);
      HttpURLConnection mWorkerWebService = (HttpURLConnection) new URL(
          "http://" + workerWebAddr.getAddress().getHostAddress() + ":"
          + mLocalTachyonCluster.getWorker().getWebLocalPort() + "/home")
          .openConnection();
      Assert.assertEquals(200, mWorkerWebService.getResponseCode());
      if (!verifyWebService(mWorkerWebService)) {
        Assert.fail("Worker web server was started but not successfully verified.");
      }
    } catch (IOException e) {
      Assert.fail("Worker web server was not successfully started.");
    }
  }
}
