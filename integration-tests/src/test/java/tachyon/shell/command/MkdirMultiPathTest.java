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
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.thrift.FileInfo;

/**
 * Test for mkdir with multi paths
 */
public class MkdirMultiPathTest extends AbstractTfsShellTest {
  @Test
  public void mkdirMultiPathTest() throws IOException, TachyonException {
    String path1 = "/testDir1";
    String path2 = "/testDir2";
    String path3 = "/testDir2/testDir2.1";
    Assert.assertEquals(0, mFsShell.run("mkdir", path1, path2, path3));

    TachyonFile tFile = mTfs.open(new TachyonURI(path1));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.isIsFolder());

    tFile = mTfs.open(new TachyonURI(path2));
    fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.isIsFolder());

    tFile = mTfs.open(new TachyonURI(path3));
    fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.isIsFolder());

  }
}
