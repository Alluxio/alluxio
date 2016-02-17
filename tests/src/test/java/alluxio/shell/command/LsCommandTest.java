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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.master.MasterContext;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for ls command.
 */
public class LsCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void lsTest() throws IOException, AlluxioException {
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    URIStatus[] files = new URIStatus[4];
    String testUser = "test_user_ls";
    clearAndLogin(testUser);

    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    files[0] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileA"));
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testDir/testFileB",
        WriteType.MUST_CACHE, 20);
    files[1] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir"));
    files[2] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir/testFileB"));
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileC", WriteType.THROUGH, 30);
    files[3] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileC"));
    mFsShell.run("ls", "/testRoot");
    String expected = "";
    expected +=
        getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10,
            LsCommand.STATE_FILE_IN_MEMORY, testUser, testUser, files[0].getPermission(),
            files[0].isFolder());
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 0,
            LsCommand.STATE_FOLDER, testUser,
            testUser, files[1].getPermission(), files[1].isFolder());
    expected +=
        getLsResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30,
            LsCommand.STATE_FILE_NOT_IN_MEMORY, testUser, testUser, files[3].getPermission(),
            files[3].isFolder());
    Assert.assertEquals(expected, mOutput.toString());
    MasterContext.reset();
  }

  @Test
  public void lsWildcardTest() throws IOException, AlluxioException {
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());
    String testUser = "test_user_lsWildcard";
    clearAndLogin(testUser);

    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    String expect = "";
    expect += getLsResultStr(new AlluxioURI("/testWildCards/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI("/testWildCards/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI("/testWildCards/foo/foobar2"), 20, testUser, testUser);
    mFsShell.run("ls", "/testWildCards/*/foo*");
    Assert.assertEquals(expect, mOutput.toString());

    expect += getLsResultStr(new AlluxioURI("/testWildCards/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI("/testWildCards/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI("/testWildCards/foo/foobar2"), 20, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI("/testWildCards/foobar4"), 40, testUser, testUser);
    mFsShell.run("ls", "/testWildCards/*");
    Assert.assertEquals(expect, mOutput.toString());
    MasterContext.reset();
  }
}
