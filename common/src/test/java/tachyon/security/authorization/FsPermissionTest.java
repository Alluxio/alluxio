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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;

public class FsPermissionTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private TachyonConf mConf = new TachyonConf();

  @Test
  public void toShortTest() throws Exception {
    FsPermission permission =
        new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
    Assert.assertEquals(0755, permission.toShort());

    permission = FsPermission.getDefault();
    Assert.assertEquals(0777, permission.toShort());

    permission = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ);
    Assert.assertEquals(0644, permission.toShort());
  }

  @Test
  public void fromShortTest() throws Exception {
    FsPermission permission = new FsPermission((short)0777);
    Assert.assertEquals(FsAction.ALL, permission.getUserAction());
    Assert.assertEquals(FsAction.ALL, permission.getGroupAction());
    Assert.assertEquals(FsAction.ALL, permission.getOtherAction());

    permission = new FsPermission((short)0644);
    Assert.assertEquals(FsAction.READ_WRITE, permission.getUserAction());
    Assert.assertEquals(FsAction.READ, permission.getGroupAction());
    Assert.assertEquals(FsAction.READ, permission.getOtherAction());

    permission = new FsPermission((short)0755);
    Assert.assertEquals(FsAction.ALL, permission.getUserAction());
    Assert.assertEquals(FsAction.READ_EXECUTE, permission.getGroupAction());
    Assert.assertEquals(FsAction.READ_EXECUTE, permission.getOtherAction());
  }

  @Test
  public void copyConstructorTest() throws Exception {
    FsPermission permission = new FsPermission(FsPermission.getDefault());
    Assert.assertEquals(FsAction.ALL, permission.getUserAction());
    Assert.assertEquals(FsAction.ALL, permission.getGroupAction());
    Assert.assertEquals(FsAction.ALL, permission.getOtherAction());
    Assert.assertEquals(0777, permission.toShort());
  }

  @Test
  public void getNoneFsPermissionTest() throws Exception {
    FsPermission permission = FsPermission.getNoneFsPermission();
    Assert.assertEquals(FsAction.NONE, permission.getUserAction());
    Assert.assertEquals(FsAction.NONE, permission.getGroupAction());
    Assert.assertEquals(FsAction.NONE, permission.getOtherAction());
    Assert.assertEquals(0000, permission.toShort());
  }

  @Test
  public void umaskTest() throws Exception {
    String umask = "0022";
    mConf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, umask);
    FsPermission umaskPermission = FsPermission.getUMask(mConf);
    // after umask 0022, 0777 should change to 0755
    FsPermission permission = FsPermission.getDefault().applyUMask(umaskPermission);
    Assert.assertEquals(FsAction.ALL, permission.getUserAction());
    Assert.assertEquals(FsAction.READ_EXECUTE, permission.getGroupAction());
    Assert.assertEquals(FsAction.READ_EXECUTE, permission.getOtherAction());
    Assert.assertEquals(0755, permission.toShort());
  }

  @Test
  public void umaskExceedLengthTest() throws Exception {
    String umask = "00022";
    mConf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, umask);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_CONFIGURATION_VALUE.getMessage(umask,
        Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK));
    FsPermission.getUMask(mConf);
  }

  @Test
  public void umaskNotIntegerTest() throws Exception {
    String umask = "NotInteger";
    mConf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, umask);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_CONFIGURATION_VALUE.getMessage(umask,
        Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK));
    FsPermission.getUMask(mConf);
  }
}
