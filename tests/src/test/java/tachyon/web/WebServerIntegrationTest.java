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
import java.util.Scanner;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

/**
 * Test the web server is up when Tachyon starts.
 *
 * <p>
 */
public class WebServerIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
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
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000);
    mLocalTachyonCluster.start();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
  }

  @Test
  public void serverUpTest() throws Exception, IOException, TachyonException {
    try {
      InetSocketAddress masterWebAddr =
          NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_WEB, mMasterTachyonConf);
      HttpURLConnection masterWebService = (HttpURLConnection) new URL(
          "http://" + masterWebAddr.getAddress().getHostAddress() + ":"
          + masterWebAddr.getPort() + "/home").openConnection();
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
      InetSocketAddress workerWebAddr =
          NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_WEB, mWorkerTachyonConf);
      HttpURLConnection workerWebService = (HttpURLConnection) new URL(
          "http://" + workerWebAddr.getAddress().getHostAddress() + ":"
          + workerWebAddr.getPort() + "/home").openConnection();
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
