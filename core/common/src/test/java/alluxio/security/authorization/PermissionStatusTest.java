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

package alluxio.security.authorization;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.group.GroupMappingService;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Tests the {@link PermissionStatus} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({GroupMappingService.Factory.class})
public final class PermissionStatusTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Tests the {@link PermissionStatus#applyUMask(FileSystemPermission)} method.
   */
  @Test
  public void applyUMaskTest() {
    FileSystemPermission umaskPermission = new FileSystemPermission((short) 0022);
    PermissionStatus permissionStatus =
        new PermissionStatus("user1", "group1", FileSystemPermission.getDefault());
    permissionStatus.applyUMask(umaskPermission);

    Assert.assertEquals(FileSystemAction.ALL, permissionStatus.getPermission().getUserAction());
    Assert.assertEquals(FileSystemAction.READ_EXECUTE,
        permissionStatus.getPermission().getGroupAction());
    Assert.assertEquals(FileSystemAction.READ_EXECUTE,
        permissionStatus.getPermission().getOtherAction());
    verifyPermissionStatus("user1", "group1", (short) 0755, permissionStatus);
  }

  /**
   * Tests the {@link PermissionStatus#defaults()} method.
   */
  @Test
  public void defaultsTest() throws Exception {
    Configuration conf = new Configuration();

    // no authentication
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    PermissionStatus permissionStatus = PermissionStatus.defaults();
    verifyPermissionStatus("", "", (short) 0777, permissionStatus);
  }

  /**
   * Tests the {@link PermissionStatus#setUserFromThriftClient(Configuration)} method.
   */
  @Test
  public void setUserFromThriftClientTest() throws Exception {
    Configuration conf = new Configuration();
    PermissionStatus permissionStatus = PermissionStatus.defaults();

    // When security is not enabled, user and group are not set
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    permissionStatus.setUserFromThriftClient(conf);
    verifyPermissionStatus("", "", (short) 0777, permissionStatus);

    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());
    AuthenticatedClientUser.set("test_client_user");

    // When authentication is enabled, user and group are inferred from thrift transport
    permissionStatus.setUserFromThriftClient(conf);
    verifyPermissionStatus("test_client_user", "test_client_user", (short) 0777, permissionStatus);
  }

  /**
   * Tests the {@link PermissionStatus#setUserFromLoginModule(Configuration)} method.
   */
  @Test
  public void setUserFromLoginModuleTest() throws Exception {
    Configuration conf = new Configuration();
    PermissionStatus permissionStatus = PermissionStatus.defaults();

    // When security is not enabled, user and group are not set
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    permissionStatus.setUserFromThriftClient(conf);
    verifyPermissionStatus("", "", (short) 0777, permissionStatus);

    // When authentication is enabled, user and group are inferred from login module
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "test_login_user");
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);

    permissionStatus.setUserFromLoginModule(conf);
    verifyPermissionStatus("test_login_user", "test_login_user", (short) 0777, permissionStatus);
  }

  private void verifyPermissionStatus(String user, String group, short permission,
      PermissionStatus permissionStatus) {
    Assert.assertEquals(user, permissionStatus.getUserName());
    Assert.assertEquals(group, permissionStatus.getGroupName());
    Assert.assertEquals(permission, permissionStatus.getPermission().toShort());
  }
}
