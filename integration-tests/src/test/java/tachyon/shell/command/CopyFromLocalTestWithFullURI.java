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
 * Test used to copy a file from local with full URL
 */
public class CopyFromLocalTestWithFullURI extends AbstractTfsShellTest {
  @Test
  public void copyFromLocalTestWithFullURI() throws IOException, TachyonException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String tachyonURI =
        "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
            + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.run("copyFromLocal", testFile.getPath(), tachyonURI);
    String cmdOut =
        getCommandOutput(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    TachyonFile file = mTfs.open(new TachyonURI("/destFileURI"));
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertEquals(10L, fileInfo.length);
    byte[] read = readContent(file, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
  }
}
