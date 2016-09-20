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

package alluxio.security;

import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.RetryHandlingBlockWorkerClient;
import alluxio.client.util.ClientTestUtils;
import alluxio.security.MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider;

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
 * Tests RPC authentication between worker and its client, in four modes: NOSASL, SIMPLE, CUSTOM,
 * KERBEROS.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class BlockWorkerClientAuthenticationIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
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
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void noAuthenticationOpenClose() throws Exception {
    authenticationOperationTest();
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE"})
  public void simpleAuthenticationOpenClose() throws Exception {
    authenticationOperationTest();
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "CUSTOM",
          PropertyKey.Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
          NameMatchAuthenticationProvider.FULL_CLASS_NAME,
          PropertyKey.Name.SECURITY_LOGIN_USERNAME, "alluxio"})
  public void customAuthenticationOpenClose() throws Exception {
    authenticationOperationTest();
  }

  @Test(timeout = 10000)
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE,
          "CUSTOM", PropertyKey.Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
          NameMatchAuthenticationProvider.FULL_CLASS_NAME,
          PropertyKey.Name.SECURITY_LOGIN_USERNAME, "alluxio"})
  public void customAuthenticationDenyConnect() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to connect to the worker");

    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), mExecutorService,
        1 /* fake session id */, true)) {
      Assert.assertFalse(blockWorkerClient.isConnected());
      // Using no-alluxio as loginUser to connect to Worker, the IOException will be thrown
      LoginUserTestUtils.resetLoginUser("no-alluxio");
      blockWorkerClient.connect();
    } finally {
      ClientTestUtils.resetClient();
    }
  }

  /**
   * Tests Alluxio Worker client connects or disconnects to the Worker.
   */
  private void authenticationOperationTest() throws Exception {
    RetryHandlingBlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(),
        mExecutorService, 1 /* fake session id */, true);

    Assert.assertFalse(blockWorkerClient.isConnected());
    blockWorkerClient.connect();
    Assert.assertTrue(blockWorkerClient.isConnected());

    blockWorkerClient.close();
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }
}
