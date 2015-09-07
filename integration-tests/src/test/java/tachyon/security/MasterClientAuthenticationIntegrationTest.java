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
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterClient;
import tachyon.master.MasterInfo;
import tachyon.security.authentication.AuthenticationFactory.AuthType;
import tachyon.security.authentication.AuthenticationProvider;

/**
 * Though its name indicates that it provides the tests for Tachyon authentication.
 * This class is likely to test four authentication modes: NOSASL, SIMPLE, CUSTOM, KERBEROS.
 *
 */
public class MasterClientAuthenticationIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private ExecutorService mExecutorService = null;
  private MasterInfo mMasterInfo = null;

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
    System.clearProperty(Constants.TACHYON_SECURITY_AUTHENTICATION);
    System.clearProperty(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS);
    System.clearProperty(Constants.TACHYON_SECURITY_USERNAME);
  }

  @Test
  public void noAuthenticationOpenCloseTest() throws Exception {
    //no authentication configure
    System.setProperty(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.NOSASL.getAuthName());
    //start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest("/file-nosasl");

    //stop cluster
    mLocalTachyonCluster.stop();
  }

  @Test
  public void simpleAuthenticationOpenCloseTest() throws Exception {
    //simple authentication configure
    System.setProperty(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.SIMPLE.getAuthName());
    //start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest("/file-simple");

    //stop cluster
    mLocalTachyonCluster.stop();
  }

  @Test
  public void customAuthenticationOpenCloseTest() throws Exception {
    //custom authentication configure
    System.setProperty(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.CUSTOM.getAuthName());
    //custom authenticationProvider configure
    System.setProperty(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        NameMatchAuthenticationProvider.class.getName());

    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master.
     */
    System.setProperty(Constants.TACHYON_SECURITY_USERNAME, "tachyon");

    //start cluster
    mLocalTachyonCluster.start();

    authenticationOperationTest("/file-custom");

    //stop cluster
    mLocalTachyonCluster.stop();
  }

  @Test
  public void customAuthenticationDenyConnectTest() throws Exception {
    //custom authentication configure
    System.setProperty(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.CUSTOM.getAuthName());
    //custom authenticationProvider configure
    System.setProperty(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        NameMatchAuthenticationProvider.class.getName());
    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master.
     */
    System.setProperty(Constants.TACHYON_SECURITY_USERNAME, "tachyon");
    //start cluster
    mLocalTachyonCluster.start();

    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    // Using no-tachyon as loginUser to connect to Master, the IOException will be thrown
    clearLoginUser();
    mThrown.expect(IOException.class);
    System.setProperty(Constants.TACHYON_SECURITY_USERNAME, "no-tachyon");
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(),
            mExecutorService, mLocalTachyonCluster.getMasterTachyonConf());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
  }

  /**
   * Test Tachyon client connects or disconnects to the Master. When the client connects
   * successfully to the Master, it can successfully create file or not.
   * @param filename
   * @throws Exception
   */
  private void authenticationOperationTest(String filename) throws Exception {
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(),
            mExecutorService, mLocalTachyonCluster.getMasterTachyonConf());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.user_createFile(filename, "", Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertNotNull(masterClient.getFileStatus(-1, filename));
    masterClient.disconnect();
    masterClient.close();
  }

  private void clearLoginUser() throws Exception {
    // User reflection to reset the private static member sLoginUser in LoginUser.
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }

  @Test
  public void kerberosAuthenticationNotSupportTest() throws Exception {
    //kerberos authentication configure
    System.setProperty(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.KERBEROS.getAuthName());
    //Currently the kerberos authentication doesn't support
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Kerberos is not supported currently");
    //start cluster
    mLocalTachyonCluster.start();
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
