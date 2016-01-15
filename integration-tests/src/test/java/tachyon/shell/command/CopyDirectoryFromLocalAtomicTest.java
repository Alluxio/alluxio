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

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.util.io.BufferUtils;

/**
 * Test used to copy a directory from local atomically
 */
public class CopyDirectoryFromLocalAtomicTest extends AbstractTfsShellTest {
  @Test
  public void copyDirectoryFromLocalAtomicTest() throws Exception {
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File testFile =
        generateFileContent("/localDir/testFile", BufferUtils.getIncreasingByteArray(10));
    File testDir = testFile.getParentFile();
    TachyonURI tachyonDirPath = new TachyonURI("/testDir");
    testFile.setReadable(false);

    String[] cmd = {"copyFromLocal", testDir.getPath(), tachyonDirPath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertNull(mTfs.openIfExists(tachyonDirPath));
    mOutput.reset();

    // If we put a copyable file in the directory, we should be able to copy just that file
    generateFileContent("/localDir/testFile2", BufferUtils.getIncreasingByteArray(20));
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertNotNull(mTfs.openIfExists(tachyonDirPath));
    Assert.assertNotNull(mTfs.openIfExists(new TachyonURI("/testDir/testFile2")));
    Assert.assertNull(mTfs.openIfExists(new TachyonURI("/testDir/testFile")));
  }
}
