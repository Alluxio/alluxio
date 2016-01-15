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
import tachyon.client.file.TachyonFile;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.util.io.BufferUtils;

/**
 * Created by dell on 2016/1/15.
 */
public class CopyFromLocalOverwriteTest extends AbstractTfsShellTest {
  @Test
  public void copyFromLocalOverwriteTest() throws Exception {
    // This tests makes sure copyFromLocal will not overwrite an existing Tachyon file
    final int LEN1 = 10;
    final int LEN2 = 20;
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(LEN1));
    File testFile2 = generateFileContent("/testFile2", BufferUtils.getIncreasingByteArray(LEN2));
    TachyonURI tachyonFilePath = new TachyonURI("/testFile");

    // Write the first file
    String[] cmd1 = {"copyFromLocal", testFile1.getPath(), tachyonFilePath.getPath()};
    mFsShell.run(cmd1);
    Assert.assertEquals(getCommandOutput(cmd1), mOutput.toString());
    mOutput.reset();
    TachyonFile tFile = mTfs.open(tachyonFilePath);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(LEN1, readContent(tFile, LEN1)));

    // Write the second file to the same location, which should cause an exception
    String[] cmd2 = {"copyFromLocal", testFile2.getPath(), tachyonFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd2));
    Assert.assertEquals(tachyonFilePath.getPath() + " already exists\n", mOutput.toString());
    // Make sure the original file is intact
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(LEN1, readContent(tFile, LEN1)));
  }
}
