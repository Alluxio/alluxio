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

import java.lang.reflect.Field;

import javax.security.sasl.AuthenticationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileSystemMasterClient;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.exception.ConnectionFailedException;
import tachyon.security.authentication.AuthenticationProvider;

/**
 * Though its name indicates that it provides the tests for Tachyon authentication. This class is
 * likely to test four authentication modes: NOSASL, SIMPLE, CUSTOM, KERBEROS.
 */
// TODO(bin): add tests for {@link LocalTachyonClusterMultiMaster} in fault tolerant mode
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public class MasterClientAuthenticationIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, 1000, Constants.GB);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    clearLoginUser();
  }

  @After
  public void after() throws Exception {
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
  }

  @Test
  @LocalTachyonClusterResource.Config(
      tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void noAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest("/file-nosasl");
  }

  @Test
  @LocalTachyonClusterResource.Config(
      tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE"})
  public void simpleAuthenticationOpenCloseTest() throws Exception {
    authenticationOperationTest("/file-simple");
  }

  @Test
  @LocalTachyonClusterResource.Config(tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE,
      "CUSTOM", Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
      NameMatchAuthenticationProvider.FULL_CLASS_NAME}, startCluster = false)
  public void customAuthenticationOpenCloseTest() throws Exception {
    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master.
     */
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon");
    mLocalTachyonClusterResource.start();
    authenticationOperationTest("/file-custom");
  }

  @Test
  @LocalTachyonClusterResource.Config(tachyonConfParams = {Constants.SECURITY_AUTHENTICATION_TYPE,
      "CUSTOM", Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
      NameMatchAuthenticationProvider.FULL_CLASS_NAME}, startCluster = false)
  public void customAuthenticationDenyConnectTest() throws Exception {
    /**
     * Using tachyon as loginUser for unit testing, only tachyon user is allowed to connect to
     * Tachyon Master.
     */
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon");
    mLocalTachyonClusterResource.start();
    // Using no-tachyon as loginUser to connect to Master, the IOException will be thrown
    clearLoginUser();
    mThrown.expect(ConnectionFailedException.class);
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "no-tachyon");
    FileSystemMasterClient masterClient =
        new FileSystemMasterClient(mLocalTachyonClusterResource.get().getMaster().getAddress(),
            mLocalTachyonClusterResource.get().getMasterTachyonConf());
    try {
      Assert.assertFalse(masterClient.isConnected());
      masterClient.connect();
    } finally {
      masterClient.close();
    }
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
        new FileSystemMasterClient(mLocalTachyonClusterResource.get().getMaster().getAddress(),
            mLocalTachyonClusterResource.get().getMasterTachyonConf());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new TachyonURI(filename), CreateFileOptions.defaults());
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
    // The fullly qualified class name of this authentication provider. This is needed to configure
    // the tachyon cluster
    public static final String FULL_CLASS_NAME =
        "tachyon.security.MasterClientAuthenticationIntegrationTest$"
            + "NameMatchAuthenticationProvider";

    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.equals("tachyon")) {
        throw new AuthenticationException("Only allow the user tachyon to connect");
      }
    }
  }
}
