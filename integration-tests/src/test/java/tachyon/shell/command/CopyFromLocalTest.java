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
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.thrift.FileInfo;
import tachyon.util.io.BufferUtils;

/**
 * Test used to copy a file from local
 */
public class CopyFromLocalTest extends AbstractTfsShellTest {
  @Test
  public void copyFromLocalTest() throws IOException, TachyonException {
    File testDir = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile =
        generateFileContent("/testDir/testFile", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        BufferUtils.getIncreasingByteArray(10, 20));

    mFsShell.run("copyFromLocal", testFile.getParent(), "/testDir");
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getParent(),
        "/testDir"}), mOutput.toString());
    TachyonFile file1 = mTfs.open(new TachyonURI("/testDir/testFile"));
    TachyonFile file2 = mTfs.open(new TachyonURI("/testDir/testDirInner/testFile2"));
    FileInfo fileInfo1 = mTfs.getInfo(file1);
    FileInfo fileInfo2 = mTfs.getInfo(file2);
    Assert.assertNotNull(fileInfo1);
    Assert.assertNotNull(fileInfo2);
    Assert.assertEquals(10, fileInfo1.length);
    Assert.assertEquals(20, fileInfo2.length);
    byte[] read = readContent(file1, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
    read = readContent(file2, 20);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, 20, read));
  }
}
