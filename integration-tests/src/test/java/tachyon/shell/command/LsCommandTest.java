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

package tachyon.shell.command;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.master.MasterContext;
import tachyon.security.LoginUser;
import tachyon.security.group.provider.IdentityUserGroupsMapping;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.shell.TfsShellUtilsTest;
import tachyon.thrift.FileInfo;

/**
 * Tests for ls command.
 */
public class LsCommandTest extends AbstractTfsShellTest {
  @Test
  public void lsTest() throws IOException, TachyonException {
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    FileInfo[] files = new FileInfo[4];
    String testUser = "test_user_ls";
    cleanAndLogin(testUser);

    TachyonFile fileA =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", TachyonStorageType.STORE,
            UnderStorageType.NO_PERSIST, 10);
    files[0] = mTfs.getInfo(fileA);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 20);
    files[1] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir")));
    TachyonFile fileC =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC", TachyonStorageType.NO_STORE,
            UnderStorageType.SYNC_PERSIST, 30);
    files[2] = mTfs.getInfo(fileC);
    mFsShell.run("ls", "/testRoot");
    String expected = "";
    expected +=
        getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10, "In Memory",
            testUser, testUser, files[0].getPermission(), files[0].isFolder);
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 0, "", testUser,
            testUser, files[1].getPermission(), files[1].isFolder);
    expected +=
        getLsResultStr("/testRoot/testFileC", files[2].getCreationTimeMs(), 30, "Not In Memory",
            testUser, testUser, files[2].getPermission(), files[2].isFolder);
    Assert.assertEquals(expected, mOutput.toString());
    // clear testing username
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.reset();
  }

  @Test
  public void lsWildcardTest() throws IOException, TachyonException {
    // clear the loginUser
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());
    String testUser = "test_user_lsWildcard";
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, testUser);

    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    String expect = "";
    expect += getLsResultStr(new TachyonURI("/testWildCards/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar2"), 20, testUser, testUser);
    mFsShell.run("ls", "/testWildCards/*/foo*");
    Assert.assertEquals(expect, mOutput.toString());

    expect += getLsResultStr(new TachyonURI("/testWildCards/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar2"), 20, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foobar4"), 40, testUser, testUser);
    mFsShell.run("ls", "/testWildCards/*");
    Assert.assertEquals(expect, mOutput.toString());
    // clear testing username
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.reset();
  }
}
