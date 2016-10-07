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

import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Tests RPC authentication between worker and its client, in four modes: NOSASL, SIMPLE, CUSTOM,
 * KERBEROS.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class BlockWorkerClientAuthenticationIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    clearLoginUser();
  }

  @After
  public void after() throws Exception {
    clearLoginUser();
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL",
      PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false"})
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
    boolean failedToConnect = false;

    // Using no-alluxio as loginUser to connect to Worker, the IOException will be thrown
    LoginUserTestUtils.resetLoginUser("no-alluxio");

    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), (long) 1 /* fake
        session id */)) {
      // Just to supress the "Empty try block" warning in CheckStyle.
      failedToConnect = false;
    } catch (IOException e) {
      if (e.getCause() instanceof TTransportException) {
        failedToConnect = true;
      }
    } finally {
      ClientTestUtils.resetClient();
    }
    Assert.assertTrue(failedToConnect);
  }

  /**
   * Tests Alluxio Worker client connects or disconnects to the Worker.
   */
  private void authenticationOperationTest() throws Exception {
    RetryHandlingBlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), (long) 1 /* fake session id */);

    blockWorkerClient.close();
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }
}
