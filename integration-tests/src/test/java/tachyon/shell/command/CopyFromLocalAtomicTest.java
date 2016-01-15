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
 * Test used to copy a file from local atomically
 */
public class CopyFromLocalAtomicTest extends AbstractTfsShellTest {
  @Test
  public void copyFromLocalAtomicTest() throws Exception {
    // copyFromLocal should not leave around any empty file metadata if it fails in the middle of
    // copying a file
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(10));
    TachyonURI tachyonFilePath = new TachyonURI("/testFile");
    // Set testFile1 to be not readable, so that when we try to open it, we fail. NOTE: for this to
    // test anything, we depend on the implementation of copyFromLocal creating the destination file
    // in Tachyon before it tries to open the source file
    testFile1.setReadable(false);
    String[] cmd = {"copyFromLocal", testFile1.getPath(), tachyonFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile1.getPath() + " (Permission denied)\n", mOutput.toString());

    // Make sure the tachyon file wasn't created anyways
    Assert.assertNull(mTfs.openIfExists(tachyonFilePath));
  }
}
