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

package alluxio.server.auth;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.master.MasterClientContext;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticationProvider;
import alluxio.security.group.GroupMappingService;
import alluxio.security.user.TestUserState;
import alluxio.security.user.UserState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.FileSystemOptions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

import javax.security.sasl.AuthenticationException;

/**
 * Though its name indicates that it provides the tests for Alluxio authentication. This class is
 * likely to test four authentication modes: NOSASL, SIMPLE, CUSTOM, KERBEROS.
 */
// TODO(bin): add tests for {@link MultiMasterLocalAlluxioCluster} in fault tolerant mode
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class MasterClientAuthenticationIntegrationTest extends BaseIntegrationTest {
  private static final String SUPERGROUP = "supergroup";
  private static final String NONSUPER = "nonsuper";
  private static final String SUPERUSER = "alluxio";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, UserGroupsMapping.class.getName())
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, SUPERGROUP)
          .build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

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
    UserState s = new TestUserState("no-alluxio", ServerConfiguration.global());
    try (FileSystemMasterClient masterClient = FileSystemMasterClient.Factory.create(
        MasterClientContext
            .newBuilder(ClientContext.create(s.getSubject(), ServerConfiguration.global()))
            .build())) {
      Assert.assertFalse(masterClient.isConnected());
      // Using no-alluxio as loginUser to connect to Master, the IOException will be thrown
      mThrown.expect(UnauthenticatedException.class);
      masterClient.connect();
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE"})
  public void simpleAuthenticationIsolatedClassLoader() throws Exception {
    FileSystemMasterClient masterClient =
        FileSystemMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    Assert.assertFalse(masterClient.isConnected());

    // Get the current context class loader to retrieve the classpath URLs.
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    // Set the context class loader to an isolated class loader.
    ClassLoader isolatedClassLoader;
    if (contextClassLoader instanceof URLClassLoader) {
      isolatedClassLoader = new URLClassLoader(((URLClassLoader) contextClassLoader).getURLs(),
          null);
    } else {
      isolatedClassLoader = new URLClassLoader(new URL[0], contextClassLoader);
    }
    Thread.currentThread().setContextClassLoader(isolatedClassLoader);
    try {
      masterClient.connect();
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
    Assert.assertTrue(masterClient.isConnected());
    masterClient.close();
  }

  /**
   * Tests Alluxio client connects or disconnects to the Master. When the client connects
   * successfully to the Master, it can successfully create file or not.
   *
   * @param filename the name of the file
   */
  private void authenticationOperationTest(String filename) throws Exception {
    UserState s = new TestUserState(SUPERUSER, ServerConfiguration.global());
    FileSystemMasterClient masterClient = FileSystemMasterClient.Factory.create(MasterClientContext
        .newBuilder(ClientContext.create(s.getSubject(), ServerConfiguration.global())).build());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new AlluxioURI(filename),
        FileSystemOptions.createFileDefaults(ServerConfiguration.global()));
    Assert.assertNotNull(
        masterClient.getStatus(new AlluxioURI(filename),
            FileSystemOptions.getStatusDefaults(ServerConfiguration.global())));
    masterClient.disconnect();
    masterClient.close();
  }

  /**
   * An authentication provider for {@link AuthType#CUSTOM}.
   */
  public static class NameMatchAuthenticationProvider implements AuthenticationProvider {
    // The fullly qualified class name of this authentication provider. This is needed to configure
    // the alluxio cluster
    public static final String FULL_CLASS_NAME =
        "alluxio.server.auth.MasterClientAuthenticationIntegrationTest$"
            + "NameMatchAuthenticationProvider";

    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.equals("alluxio")) {
        throw new AuthenticationException("Only allow the user alluxio to connect");
      }
    }
  }

  public static class UserGroupsMapping implements GroupMappingService {
    public UserGroupsMapping() {}

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (user.equals(SUPERUSER)) {
        return Collections.singletonList(SUPERGROUP);
      }
      return Collections.singletonList(NONSUPER);
    }
  }
}
