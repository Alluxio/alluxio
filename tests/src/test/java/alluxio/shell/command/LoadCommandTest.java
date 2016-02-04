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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import alluxio.TachyonURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.exception.TachyonException;
import alluxio.shell.AbstractTfsShellTest;

/**
 * Tests for load command.
 */
public class LoadCommandTest extends AbstractTfsShellTest {
  @Test
  public void loadDirTest() throws IOException, TachyonException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.THROUGH, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileB", WriteType.MUST_CACHE, 10);
    TachyonURI uriA = new TachyonURI("/testRoot/testFileA");
    TachyonURI uriB = new TachyonURI("/testRoot/testFileB");

    URIStatus statusA = mFileSystem.getStatus(uriA);
    URIStatus statusB = mFileSystem.getStatus(uriB);
    Assert.assertFalse(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("load", "/testRoot");
    statusA = mFileSystem.getStatus(uriA);
    statusB = mFileSystem.getStatus(uriB);
    Assert.assertTrue(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadFileTest() throws IOException, TachyonException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.THROUGH, 10);
    TachyonURI uri = new TachyonURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertFalse(status.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("load", "/testFile");
    status = mFileSystem.getStatus(uri);
    Assert.assertTrue(status.getInMemoryPercentage() == 100);
  }
}
