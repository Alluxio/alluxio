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
import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterContext;
import tachyon.security.authentication.AuthType;
import tachyon.worker.ClientMetrics;
import tachyon.worker.WorkerClient;
import tachyon.worker.WorkerContext;

/**
 * Test RPC authentication between worker and its client, in four modes: NOSASL, SIMPLE, CUSTOM,
 * KERBEROS.
 *
 * TODO: the way to set and isolate MasterContext/WorkerContext across testcases is hacky. A better
 * solution is needed.
 */
public class WorkerClientAuthenticationIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster;
  private ExecutorService mExecutorService;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mExecutorService = Executors.newFixedThreadPool(2);
    clearLoginUser();
  }

  @After
  public void after() throws Exception {
    // stop cluster
    mLocalTachyonCluster.stop();

    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.resetConf();
    WorkerContext.resetConf();
  }

  @Test
  public void noAuthenticationOpenCloseTest() throws Exception {
    // no authentication type configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.NOSASL.getAuthName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.NOSASL.getAuthName());

    // start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest();

    // stop cluster
    mLocalTachyonCluster.stop();
  }

  @Test
  public void simpleAuthenticationOpenCloseTest() throws Exception {
    // simple authentication type configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.SIMPLE.getAuthName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.SIMPLE.getAuthName());

    // start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest();

    // stop cluster
    mLocalTachyonCluster.stop();
  }

  @Test
  public void customAuthenticationOpenCloseTest() throws Exception {
    // custom authentication type configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.CUSTOM.getAuthName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.CUSTOM.getAuthName());
    // custom authenticationProvider configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider.class.getName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider.class.getName());

    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Worker.
     */
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon");

    // start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest();

    // stop cluster
    mLocalTachyonCluster.stop();
  }

  @Test
  public void customAuthenticationDenyConnectTest() throws Exception {
    // custom authentication type configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.CUSTOM.getAuthName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.CUSTOM.getAuthName());
    // custom authenticationProvider configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider.class.getName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        MasterClientAuthenticationIntegrationTest.NameMatchAuthenticationProvider.class.getName());

    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master during starting cluster.
     */
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon");
    // start cluster
    mLocalTachyonCluster.start();

    // Using no-tachyon as loginUser to connect to Worker, the IOException will be thrown
    clearLoginUser();
    mThrown.expect(IOException.class);
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "no-tachyon");

    WorkerClient workerClient =
        new WorkerClient(mLocalTachyonCluster.getWorkerAddress(), mExecutorService,
            mLocalTachyonCluster.getWorkerTachyonConf(), 1 /* fake session id */, true,
            new ClientMetrics());
    Assert.assertFalse(workerClient.isConnected());
    workerClient.mustConnect();
  }

  /**
   * Test Tachyon Worker client connects or disconnects to the Worker.
   *
   * @throws Exception
   */
  private void authenticationOperationTest() throws Exception {
    WorkerClient workerClient =
        new WorkerClient(mLocalTachyonCluster.getWorkerAddress(), mExecutorService,
            mLocalTachyonCluster.getWorkerTachyonConf(), 1 /* fake session id */, true,
            new ClientMetrics());

    Assert.assertFalse(workerClient.isConnected());
    workerClient.mustConnect();
    Assert.assertTrue(workerClient.isConnected());

    workerClient.close();
  }

  private void clearLoginUser() throws Exception {
    // User reflection to reset the private static member sLoginUser in LoginUser.
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }
}
