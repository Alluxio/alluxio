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
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.permission.AclEntry.AclPermission;
import tachyon.master.permission.AclEntry.AclType;

public class AclTest {

  private AclEntry userEntry;
  private AclEntry groupEntry;
  private AclEntry otherEntry;
  private Acl acl;

  @Before
  public void setup() throws Exception {
    userEntry = new AclEntry.Builder()
      .setType(AclType.USER)
      .setName("user1")
      .setPermission(AclPermission.READ_WRITE)
      .build();
    groupEntry = new AclEntry.Builder()
      .setType(AclType.GROUP)
      .setName("group1")
      .setPermission(AclPermission.READ_WRITE)
      .build();
    otherEntry = new AclEntry.Builder()
      .setType(AclType.OTHER)
      .setPermission(AclPermission.READ_WRITE)
      .build();

    // rw-rw-rw-
    // 0666
    acl = new Acl.Builder()
      .setUserEntry(userEntry)
      .setGroupEntry(groupEntry)
      .setOtherEntry(otherEntry)
      .build();
  }

  @Test
  public void testBasic() throws Exception {
    Assert.assertEquals("user1", acl.getUserName());
    Assert.assertEquals("group1", acl.getGroupName());
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getUserPermission());
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getGroupPermission());
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getOtherPermission());
    Assert.assertEquals(0666, acl.toShort());
  }

  @Test
  public void testUmask() throws Exception {
    short umask = 0002;
    acl.umask(umask);
    // after umask 0002, 0666 should change to 0664
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getUserPermission());
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getGroupPermission());
    Assert.assertEquals(AclPermission.READ, acl.getOtherPermission());
    Assert.assertEquals(0664, acl.toShort());

    TachyonConf conf = new TachyonConf();
    conf.set(Constants.FS_PERMISSIONS_UMASK_KEY, String.format("%d", 0022));
    acl.umask(conf);
    // after umask 0022, 0664 should change to 0644
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getUserPermission());
    Assert.assertEquals(AclPermission.READ, acl.getGroupPermission());
    Assert.assertEquals(AclPermission.READ, acl.getOtherPermission());
    Assert.assertEquals(0644, acl.toShort());
  }

  @Test
  public void testSetPermission() throws Exception {
    short mode = 0644;
    acl.setPermission(mode);
    // after chmod to 0644, acl's permission should change to rw-r--r--
    Assert.assertEquals(AclPermission.READ_WRITE, acl.getUserPermission());
    Assert.assertEquals(AclPermission.READ, acl.getGroupPermission());
    Assert.assertEquals(AclPermission.READ, acl.getOtherPermission());
    Assert.assertEquals(0644, acl.toShort());

    mode = 0777;
    acl.setPermission(mode);
    // after chmod to 0777, acl's permission should change to rwxrwxrwx
    Assert.assertEquals(AclPermission.ALL, acl.getUserPermission());
    Assert.assertEquals(AclPermission.ALL, acl.getGroupPermission());
    Assert.assertEquals(AclPermission.ALL, acl.getOtherPermission());
    Assert.assertEquals(0777, acl.toShort());
  }

  @Test
  public void testSetOwner() throws Exception {
    // chown user's owner to 'user2'
    acl.setUserOwner("user2");
    Assert.assertEquals("user2", acl.getUserName());
    // chown group's owner to 'group2'
    acl.setGroupOwner("group2");
    Assert.assertEquals("group2", acl.getGroupName());
  }
}
