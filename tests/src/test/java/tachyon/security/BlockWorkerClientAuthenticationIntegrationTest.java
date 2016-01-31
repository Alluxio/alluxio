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

package tachyon.security;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.client.ClientContext;
import tachyon.client.block.BlockWorkerClient;
import tachyon.client.util.ClientTestUtils;
import tachyon.security.MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider;
import tachyon.worker.ClientMetrics;

/**
 * Test RPC authentication between worker and its client, in four modes: NOSASL, SIMPLE, CUSTOM,
 * KERBEROS.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public class BlockWorkerClientAuthenticationIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, Constants.GB);
  private ExecutorService mExecutorService;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mExecutorService = Executors.newFixedThreadPool(2);
    clearLoginUser();
  }

  @After
  public void after() throws Exception {
    clearLoginUser();
    mExecutorService.shutdownNow();
  }

  @Test
  @LocalTachyonClusterResource.Config(
      tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void noAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest();
  }

  @Test
  @LocalTachyonClusterResource.Config(
      tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE"})
  public void simpleAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest();
  }

  @Test
  @LocalTachyonClusterResource.Config(
      tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM",
          Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
          NameMatchAuthenticationProvider.FULL_CLASS_NAME,
          Constants.SECURITY_LOGIN_USERNAME, "tachyon"})
  public void customAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest();
  }

  @Test(timeout = 10000)
  @LocalTachyonClusterResource.Config(tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE,
      "CUSTOM", Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
      NameMatchAuthenticationProvider.FULL_CLASS_NAME,
      Constants.SECURITY_LOGIN_USERNAME, "tachyon"})
  public void customAuthenticationDenyConnectTest() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to connect to the worker");

    BlockWorkerClient blockWorkerClient = new BlockWorkerClient(
        mLocalTachyonClusterResource.get().getWorkerAddress(),
        mExecutorService, ClientContext.getConf(),
        1 /* fake session id */, true, new ClientMetrics());
    try {
      Assert.assertFalse(blockWorkerClient.isConnected());
      // Using no-tachyon as loginUser to connect to Worker, the IOException will be thrown
      LoginUserTestUtils.resetLoginUser(ClientContext.getConf(), "no-tachyon");
      blockWorkerClient.connect();
    } finally {
      blockWorkerClient.close();
      ClientTestUtils.resetClientContext();
    }
  }

  /**
   * Test Tachyon Worker client connects or disconnects to the Worker.
   *
   * @throws Exception
   */
  private void authenticationOperationTest() throws Exception {
    BlockWorkerClient blockWorkerClient = new BlockWorkerClient(
        mLocalTachyonClusterResource.get().getWorkerAddress(),
        mExecutorService, mLocalTachyonClusterResource.get().getWorkerTachyonConf(),
        1 /* fake session id */, true, new ClientMetrics());

    Assert.assertFalse(blockWorkerClient.isConnected());
    blockWorkerClient.connect();
    Assert.assertTrue(blockWorkerClient.isConnected());

    blockWorkerClient.close();
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }
}
