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
package tachyon.master.permission;

import org.junit.Assert;
import org.junit.Test;

import tachyon.master.permission.AclEntry.AclPermission;

public class AclUtilTest {
  private String s[] = {"---", "--x", "-w-", "-wx", "r--", "r-x" ,"rw-", "rwx"};
  private AclPermission p[] = {AclPermission.NONE, AclPermission.EXECUTE,
      AclPermission.WRITE, AclPermission.WRITE_EXECUTE, AclPermission.READ,
      AclPermission.READ_EXECUTE, AclPermission.READ_WRITE, AclPermission.ALL};

  @Test
  public void testGetPermissionFromShort() throws Exception {
    short mode = 0777;
    AclPermission userPermission = AclUtil.toUserPermission(mode);
    AclPermission groupPermission = AclUtil.toGroupPermission(mode);
    AclPermission otherPermission = AclUtil.toOtherPermission(mode);
    Assert.assertEquals(AclPermission.ALL, userPermission);
    Assert.assertEquals(AclPermission.ALL, groupPermission);
    Assert.assertEquals(AclPermission.ALL, otherPermission);

    mode = 0000;
    userPermission = AclUtil.toUserPermission(mode);
    groupPermission = AclUtil.toGroupPermission(mode);
    otherPermission = AclUtil.toOtherPermission(mode);
    Assert.assertEquals(AclPermission.NONE, userPermission);
    Assert.assertEquals(AclPermission.NONE, groupPermission);
    Assert.assertEquals(AclPermission.NONE, otherPermission);

    mode = 0123;
    userPermission = AclUtil.toUserPermission(mode);
    groupPermission = AclUtil.toGroupPermission(mode);
    otherPermission = AclUtil.toOtherPermission(mode);
    Assert.assertEquals(AclPermission.EXECUTE, userPermission);
    Assert.assertEquals(AclPermission.WRITE, groupPermission);
    Assert.assertEquals(AclPermission.WRITE_EXECUTE, otherPermission);
  }

  @Test
  public void testGetPermissionFromString() throws Exception {
    for (int i = 0; i < 7; i++) {
      AclPermission permission = AclUtil.getPermission(s[i]);
      Assert.assertEquals(p[i], permission);
    }
  }
}
