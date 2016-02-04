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

package alluxio.security.authorization;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.PlainSaslServer;
import alluxio.security.group.GroupMappingService;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

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
   * Tests the {@link PermissionStatus#getDirDefault()} method.
   */
  @Test
  public void permissionStatusTest() {
    PermissionStatus permissionStatus =
        new PermissionStatus("user1", "group1", FileSystemPermission.getDefault());

    verifyPermissionStatus("user1", "group1", (short) 0777, permissionStatus);

    permissionStatus = PermissionStatus.getDirDefault();

    verifyPermissionStatus("", "", (short) 0777, permissionStatus);
  }

  /**
   * Tests the {@link PermissionStatus#applyUMask(FileSystemPermission)} method.
   */
  @Test
  public void applyUMaskTest() {
    FileSystemPermission umaskPermission = new FileSystemPermission((short)0022);
    PermissionStatus permissionStatus =
        new PermissionStatus("user1", "group1", FileSystemPermission.getDefault());
    permissionStatus = permissionStatus.applyUMask(umaskPermission);

    Assert.assertEquals(FileSystemAction.ALL, permissionStatus.getPermission().getUserAction());
    Assert.assertEquals(FileSystemAction.READ_EXECUTE,
        permissionStatus.getPermission().getGroupAction());
    Assert.assertEquals(FileSystemAction.READ_EXECUTE,
        permissionStatus.getPermission().getOtherAction());
    Assert.assertEquals(0755, permissionStatus.getPermission().toShort());
  }

  /**
   * Tests the {@link PermissionStatus#get(Configuration, boolean)} method.
   */
  @Test
  public void getPermissionStatusTest() throws Exception {
    Configuration conf = new Configuration();
    PermissionStatus permissionStatus;

    // no authentication
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    permissionStatus = PermissionStatus.get(conf, true);
    verifyPermissionStatus("", "", (short) 0000, permissionStatus);

    // authentication is enabled, and remote is true
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    PlainSaslServer.AuthorizedClientUser.set("test_client_user");
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());
    permissionStatus = PermissionStatus.get(conf, true);
    verifyPermissionStatus("test_client_user", "test_client_user", (short) 0755, permissionStatus);

    // authentication is enabled, and remote is false
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "test_login_user");
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());
    permissionStatus = PermissionStatus.get(conf, false);
    verifyPermissionStatus("test_login_user", "test_login_user", (short) 0755, permissionStatus);
  }

  /**
   * Tests that retrieving the {@link PermissionStatus} with multiple groups works as expected.
   */
  @Test
  public void getPermissionStatusWithMultiGroupsTest() throws Exception {
    // mock a multi-groups test case
    Configuration conf = new Configuration();
    PermissionStatus permissionStatus;
    GroupMappingService groupService = PowerMockito.mock(GroupMappingService.class);
    PowerMockito.when(groupService.getGroups(Mockito.anyString())).thenReturn(
        Lists.newArrayList("group1", "group2"));
    PowerMockito.mockStatic(GroupMappingService.Factory.class);
    Mockito.when(
        GroupMappingService.Factory.getUserToGroupsMappingService(Mockito.any(Configuration.class)))
        .thenReturn(groupService);

    // no authentication
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    permissionStatus = PermissionStatus.get(conf, true);
    verifyPermissionStatus("", "", (short) 0000, permissionStatus);

    // authentication is enabled, and remote is true
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    PlainSaslServer.AuthorizedClientUser.set("test_client_user");
    permissionStatus = PermissionStatus.get(conf, true);
    verifyPermissionStatus("test_client_user", "group1", (short) 0755, permissionStatus);

    // authentication is enabled, and remote is false
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "test_login_user");
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());
    permissionStatus = PermissionStatus.get(conf, false);
    verifyPermissionStatus("test_login_user", "group1", (short) 0755, permissionStatus);
  }

  /**
   * Tests that retrieving the {@link PermissionStatus} with empty group works as expected.
   */
  @Test
  public void getPermissionStatusWithEmptyGroupsTest() throws Exception {
    // mock an empty group test case
    Configuration conf = new Configuration();
    PermissionStatus permissionStatus;
    GroupMappingService groupService = PowerMockito.mock(GroupMappingService.class);
    PowerMockito.when(groupService.getGroups(Mockito.anyString())).thenReturn(
        Lists.newArrayList(""));
    PowerMockito.mockStatic(GroupMappingService.Factory.class);
    Mockito.when(
        GroupMappingService.Factory.getUserToGroupsMappingService(Mockito.any(Configuration.class)))
        .thenReturn(groupService);

    // no authentication
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    permissionStatus = PermissionStatus.get(conf, true);
    verifyPermissionStatus("", "", (short) 0000, permissionStatus);

    // authentication is enabled, and remote is true
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    PlainSaslServer.AuthorizedClientUser.set("test_client_user");
    permissionStatus = PermissionStatus.get(conf, true);
    verifyPermissionStatus("test_client_user", "", (short) 0755, permissionStatus);
  }

  private void verifyPermissionStatus(String user, String group, short permission,
      PermissionStatus permissionStatus) {
    Assert.assertEquals(user, permissionStatus.getUserName());
    Assert.assertEquals(group, permissionStatus.getGroupName());
    Assert.assertEquals(permission, permissionStatus.getPermission().toShort());
  }
}
