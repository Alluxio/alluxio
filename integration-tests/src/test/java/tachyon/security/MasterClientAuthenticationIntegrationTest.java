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

import javax.security.sasl.AuthenticationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.client.FileSystemMasterClient;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterContext;
import tachyon.security.authentication.AuthType;
import tachyon.security.authentication.AuthenticationProvider;
import tachyon.worker.WorkerContext;

/**
 * Though its name indicates that it provides the tests for Tachyon authentication. This class is
 * likely to test four authentication modes: NOSASL, SIMPLE, CUSTOM, KERBEROS.
 *
 * TODO: add tests for {@link tachyon.master.LocalTachyonClusterMultiMaster} in fault tolerant mode
 */
public class MasterClientAuthenticationIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private ExecutorService mExecutorService = null;

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

    authenticationOperationTest("/file-nosasl");

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

    authenticationOperationTest("/file-simple");

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
        NameMatchAuthenticationProvider.class.getName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        NameMatchAuthenticationProvider.class.getName());

    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master.
     */
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon");

    // start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest("/file-custom");

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
        NameMatchAuthenticationProvider.class.getName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        NameMatchAuthenticationProvider.class.getName());

    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master.
     */
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon");
    // start cluster
    mLocalTachyonCluster.start();

    // Using no-tachyon as loginUser to connect to Master, the IOException will be thrown
    clearLoginUser();
    mThrown.expect(IOException.class);
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "no-tachyon");
    FileSystemMasterClient masterClient =
        new FileSystemMasterClient(mLocalTachyonCluster.getMaster().getAddress(), mExecutorService,
            mLocalTachyonCluster.getMasterTachyonConf());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
  }

  @Test
  public void kerberosAuthenticationNotSupportTest() throws Exception {
    // kerberos authentication type configure
    MasterContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.KERBEROS.getAuthName());
    WorkerContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.KERBEROS.getAuthName());

    // Currently the kerberos authentication doesn't support
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Kerberos is not supported currently");
    // start cluster
    mLocalTachyonCluster.start();
  }

  /**
   * Test Tachyon client connects or disconnects to the Master. When the client connects
   * successfully to the Master, it can successfully create file or not.
   *
   * @param filename
   * @throws Exception
   */
  private void authenticationOperationTest(String filename) throws Exception {
    FileSystemMasterClient masterClient =
        new FileSystemMasterClient(mLocalTachyonCluster.getMaster().getAddress(), mExecutorService,
            mLocalTachyonCluster.getMasterTachyonConf());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(filename, Constants.DEFAULT_BLOCK_SIZE_BYTE, true, Constants.NO_TTL);
    Assert.assertNotNull(masterClient.getFileId(filename));
    masterClient.disconnect();
    masterClient.close();
  }

  private void clearLoginUser() throws Exception {
    // User reflection to reset the private static member sLoginUser in LoginUser.
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }


  public static class NameMatchAuthenticationProvider implements AuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.equals("tachyon")) {
        throw new AuthenticationException("Only allow the user tachyon to connect");
      }
    }
  }
}
