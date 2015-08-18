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

package tachyon.security.authorization;

import org.junit.Assert;
import org.junit.Test;

public class PermissionStatusTest {

  @Test
  public void permissionStatusTest() {
    PermissionStatus permissionStatus =
        new PermissionStatus("user1", "group1", FsPermission.getDefault());

    Assert.assertEquals("user1", permissionStatus.getUserName());
    Assert.assertEquals("group1", permissionStatus.getGroupName());
    Assert.assertEquals(0777, permissionStatus.getPermission().toShort());
  }

  @Test
  public void applyUMaskTest() {
    FsPermission umaskPermission = new FsPermission((short)0022);
    PermissionStatus permissionStatus =
        new PermissionStatus("user1", "group1", FsPermission.getDefault());
    permissionStatus = permissionStatus.applyUMask(umaskPermission);

    Assert.assertEquals(FsAction.ALL, permissionStatus.getPermission().getUserAction());
    Assert.assertEquals(FsAction.READ_EXECUTE, permissionStatus.getPermission().getGroupAction());
    Assert.assertEquals(FsAction.READ_EXECUTE, permissionStatus.getPermission().getOtherAction());
    Assert.assertEquals(0755, permissionStatus.getPermission().toShort());

  }
}
