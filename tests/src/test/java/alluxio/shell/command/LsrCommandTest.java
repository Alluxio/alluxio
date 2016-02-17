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
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.master.MasterContext;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for lsr command.
 */
public class LsrCommandTest extends AbstractAlluxioShellTest {
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          Constants.SECURITY_GROUP_MAPPING,
          "alluxio.security.group.provider.IdentityUserGroupsMapping"})
  public void lsrTest() throws IOException, AlluxioException {
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    URIStatus[] files = new URIStatus[4];
    String testUser = "test_user_lsr";
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
    mFsShell.run("lsr", "/testRoot");
    String expected = "";
    expected += "WARNING: lsr is deprecated. Please use ls -R instead.\n";
    expected +=
        getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10, "In Memory",
            testUser, testUser, files[0].getPermission(), files[0].isFolder());
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 0, "", testUser,
            testUser, files[1].getPermission(), files[1].isFolder());
    expected +=
        getLsResultStr("/testRoot/testDir/testFileB", files[2].getCreationTimeMs(), 20,
            "In Memory", testUser, testUser, files[2].getPermission(), files[2].isFolder());
    expected +=
        getLsResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30, "Not In Memory",
            testUser, testUser, files[3].getPermission(), files[3].isFolder());
    Assert.assertEquals(expected, mOutput.toString());
    // clear testing username
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.reset();
  }
}
