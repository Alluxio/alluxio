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

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.ConnectionFailedException;
import alluxio.security.authentication.AuthenticationProvider;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.sasl.AuthenticationException;

/**
 * Though its name indicates that it provides the tests for Alluxio authentication. This class is
 * likely to test four authentication modes: NOSASL, SIMPLE, CUSTOM, KERBEROS.
 */
// TODO(bin): add tests for {@link MultiMasterLocalAlluxioCluster} in fault tolerant mode
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class MasterClientAuthenticationIntegrationTest {
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
    authenticationOperationTest("/file-nosasl");
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE"})
  public void simpleAuthenticationOpenClose() throws Exception {
    authenticationOperationTest("/file-simple");
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "CUSTOM",
          PropertyKey.Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
          NameMatchAuthenticationProvider.FULL_CLASS_NAME,
          PropertyKey.Name.SECURITY_LOGIN_USERNAME, "alluxio"})
  public void customAuthenticationOpenClose() throws Exception {
    authenticationOperationTest("/file-custom");
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "CUSTOM",
          PropertyKey.Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
          NameMatchAuthenticationProvider.FULL_CLASS_NAME,
          PropertyKey.Name.SECURITY_LOGIN_USERNAME, "alluxio"})
  public void customAuthenticationDenyConnect() throws Exception {
    mThrown.expect(ConnectionFailedException.class);

    try (FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress())) {
      Assert.assertFalse(masterClient.isConnected());
      // Using no-alluxio as loginUser to connect to Master, the IOException will be thrown
      LoginUserTestUtils.resetLoginUser("no-alluxio");
      masterClient.connect();
    }
  }

  /**
   * Tests Alluxio client connects or disconnects to the Master. When the client connects
   * successfully to the Master, it can successfully create file or not.
   *
   * @param filename the name of the file
   * @throws Exception if a {@link FileSystemMasterClient} operation fails
   */
  private void authenticationOperationTest(String filename) throws Exception {
    FileSystemMasterClient masterClient =
        new FileSystemMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new AlluxioURI(filename), CreateFileOptions.defaults());
    Assert.assertNotNull(masterClient.getStatus(new AlluxioURI(filename)));
    masterClient.disconnect();
    masterClient.close();
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }

  public static class NameMatchAuthenticationProvider implements AuthenticationProvider {
    // The fullly qualified class name of this authentication provider. This is needed to configure
    // the alluxio cluster
    public static final String FULL_CLASS_NAME =
        "alluxio.security.MasterClientAuthenticationIntegrationTest$"
            + "NameMatchAuthenticationProvider";

    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.equals("alluxio")) {
        throw new AuthenticationException("Only allow the user alluxio to connect");
      }
    }
  }
}
