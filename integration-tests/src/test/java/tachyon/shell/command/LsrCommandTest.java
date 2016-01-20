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

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.master.MasterContext;
import tachyon.security.group.provider.IdentityUserGroupsMapping;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.thrift.FileInfo;

/**
 * Test for lsr command.
 */
public class LsrCommandTest extends AbstractTfsShellTest {
  @Test
  public void lsrTest() throws IOException, TachyonException {
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    FileInfo[] files = new FileInfo[4];
    String testUser = "test_user_lsr";
    cleanAndLogin(testUser);

    TachyonFile fileA =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", TachyonStorageType.STORE,
            UnderStorageType.NO_PERSIST, 10);
    files[0] = mTfs.getInfo(fileA);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 20);
    files[1] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir")));
    files[2] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir/testFileB")));
    TachyonFile fileC =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC", TachyonStorageType.NO_STORE,
            UnderStorageType.SYNC_PERSIST, 30);
    files[3] = mTfs.getInfo(fileC);
    mFsShell.run("lsr", "/testRoot");
    String expected = "";
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
    MasterContext.reset();
  }
}
