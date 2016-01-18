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

import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.thrift.FileInfo;

/**
 * Tests for load command.
 */
public class LoadCommandTest extends AbstractTfsShellTest {
  @Test
  public void loadDirTest() throws IOException, TachyonException {
    TachyonFile fileA =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", TachyonStorageType.NO_STORE,
            UnderStorageType.SYNC_PERSIST, 10);
    TachyonFile fileB =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", TachyonStorageType.STORE,
            UnderStorageType.NO_PERSIST, 10);
    FileInfo fileInfoA = mTfs.getInfo(fileA);
    FileInfo fileInfoB = mTfs.getInfo(fileB);
    Assert.assertFalse(fileInfoA.getInMemoryPercentage() == 100);
    Assert.assertTrue(fileInfoB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("load", "/testRoot");
    fileInfoA = mTfs.getInfo(fileA);
    fileInfoB = mTfs.getInfo(fileB);
    Assert.assertTrue(fileInfoA.getInMemoryPercentage() == 100);
    Assert.assertTrue(fileInfoB.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadFileTest() throws IOException, TachyonException {
    TachyonFile file =
        TachyonFSTestUtils.createByteFile(mTfs, "/testFile", TachyonStorageType.NO_STORE,
            UnderStorageType.SYNC_PERSIST, 10);
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertFalse(fileInfo.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("load", "/testFile");
    fileInfo = mTfs.getInfo(file);
    Assert.assertTrue(fileInfo.getInMemoryPercentage() == 100);
  }
}
