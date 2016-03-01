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

package alluxio.security;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.util.ClientTestUtils;
import alluxio.security.MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider;
import alluxio.worker.ClientMetrics;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test RPC authentication between worker and its client, in four modes: NOSASL, SIMPLE, CUSTOM,
 * KERBEROS.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class BlockWorkerClientAuthenticationIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
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
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void noAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest();
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE"})
  public void simpleAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest();
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM",
          Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
          NameMatchAuthenticationProvider.FULL_CLASS_NAME,
          Constants.SECURITY_LOGIN_USERNAME, "alluxio"})
  public void customAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest();
  }

  @Test(timeout = 10000)
  @LocalAlluxioClusterResource.Config(confParams = {Constants.SECURITY_AUTHENTICATION_TYPE,
      "CUSTOM", Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
      NameMatchAuthenticationProvider.FULL_CLASS_NAME,
      Constants.SECURITY_LOGIN_USERNAME, "alluxio"})
  public void customAuthenticationDenyConnectTest() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to connect to the worker");

    BlockWorkerClient blockWorkerClient = new BlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(),
        mExecutorService, ClientContext.getConf(),
        1 /* fake session id */, true, new ClientMetrics());
    try {
      Assert.assertFalse(blockWorkerClient.isConnected());
      // Using no-alluxio as loginUser to connect to Worker, the IOException will be thrown
      LoginUserTestUtils.resetLoginUser(ClientContext.getConf(), "no-alluxio");
      blockWorkerClient.connect();
    } finally {
      blockWorkerClient.close();
      ClientTestUtils.resetClientContext();
    }
  }

  /**
   * Test Alluxio Worker client connects or disconnects to the Worker.
   *
   * @throws Exception
   */
  private void authenticationOperationTest() throws Exception {
    BlockWorkerClient blockWorkerClient = new BlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(),
        mExecutorService, mLocalAlluxioClusterResource.get().getWorkerConf(),
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
