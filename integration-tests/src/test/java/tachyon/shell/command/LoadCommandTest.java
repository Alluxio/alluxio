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

import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.client.file.URIStatus;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests for load command.
 */
public class LoadCommandTest extends AbstractTfsShellTest {
  @Test
  public void loadDirTest() throws IOException, TachyonException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.THROUGH, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", WriteType.MUST_CACHE, 10);
    TachyonURI uriA = new TachyonURI("/testRoot/testFileA");
    TachyonURI uriB = new TachyonURI("/testRoot/testFileB");

    URIStatus statusA = mTfs.getStatus(uriA);
    URIStatus statusB = mTfs.getStatus(uriB);
    Assert.assertFalse(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("load", "/testRoot");
    statusA = mTfs.getStatus(uriA);
    statusB = mTfs.getStatus(uriB);
    Assert.assertTrue(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadFileTest() throws IOException, TachyonException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.THROUGH, 10);
    TachyonURI uri = new TachyonURI("/testFile");
    URIStatus status = mTfs.getStatus(uri);
    Assert.assertFalse(status.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("load", "/testFile");
    status = mTfs.getStatus(uri);
    Assert.assertTrue(status.getInMemoryPercentage() == 100);
  }
}
