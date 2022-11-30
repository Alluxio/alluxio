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

package alluxio.master.service.web;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.PortReservationRule;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.MasterWebServer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Tests for the different implementations of WebServerSimpleService.
 */
public class WebServerSimpleServiceTest {
  @Rule
  public PortReservationRule mPort = new PortReservationRule();

  private InetSocketAddress mWebAddress;
  private AlluxioMasterProcess mMasterProcess;

  @Before
  public void setUp() {
    mMasterProcess = Mockito.mock(AlluxioMasterProcess.class);
    mWebAddress = new InetSocketAddress(mPort.getPort());
    Mockito.when(mMasterProcess.createWebServer()).thenAnswer(mock -> new MasterWebServer(
        NetworkAddressUtils.ServiceType.MASTER_WEB.getServiceName(), mWebAddress, mMasterProcess));
  }

  @Test
  public void whenLeadingTest() {
    Configuration.set(PropertyKey.STANDBY_MASTER_WEB_ENABLED, false);
    WebServerSimpleService webService =
        WebServerSimpleService.Factory.create(mWebAddress, mMasterProcess);
    Assert.assertTrue(webService instanceof WhenLeadingWebServerSimpleService);
    Assert.assertTrue(waitForFree());

    Assert.assertFalse(webService.isServing());
    webService.start();
    Assert.assertTrue(isBound());
    Assert.assertFalse(webService.isServing());
    for (int i = 0; i < 5; i++) {
      webService.promote();
      Assert.assertTrue(webService.isServing());
      Assert.assertTrue(isBound());
      webService.demote();
      Assert.assertTrue(isBound());
      Assert.assertFalse(webService.isServing());
    }
    webService.stop();
    Assert.assertFalse(webService.isServing());
    Assert.assertFalse(isBound());
  }

  @Test
  public void alwaysOnTest() {
    Configuration.set(PropertyKey.STANDBY_MASTER_WEB_ENABLED, true);
    WebServerSimpleService webService =
        WebServerSimpleService.Factory.create(mWebAddress, mMasterProcess);
    Assert.assertTrue(webService instanceof AlwaysOnWebServerSimpleService);
    Assert.assertTrue(waitForFree());

    Assert.assertFalse(webService.isServing());
    webService.start();
    Assert.assertTrue(webService.isServing());
    Assert.assertTrue(isBound());
    for (int i = 0; i < 5; i++) {
      webService.promote();
      Assert.assertTrue(webService.isServing());
      Assert.assertTrue(isBound());
      webService.demote();
      Assert.assertTrue(webService.isServing());
      Assert.assertTrue(isBound());
    }
    webService.stop();
    Assert.assertTrue(waitForFree());
    Assert.assertFalse(webService.isServing());
  }

  private boolean isBound() {
    try (Socket socket = new Socket(mWebAddress.getAddress(), mWebAddress.getPort())) {
      return true;
    } catch (ConnectException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean waitForFree() {
    try {
      CommonUtils.waitFor("wait for socket to be free", () -> !isBound(),
          WaitForOptions.defaults().setTimeoutMs(1_000).setInterval(10));
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
