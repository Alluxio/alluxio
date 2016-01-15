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

/**
 * Test used to copy from a local file to destination path
 */
public class CopyFromLocalFileToDstPathTest extends AbstractTfsShellTest {
  @Test
  public void copyFromLocalFileToDstPathTest() throws IOException, TachyonException {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.run("mkdir", "/dstDir");
    mFsShell.run("copyFromLocal", localFile.getPath(), "/dstDir");

    TachyonFile file = mTfs.open(new TachyonURI("/dstDir/testFile"));
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertNotNull(fileInfo);
    byte[] read = readContent(file, data.length);
    Assert.assertEquals(new String(read), dataString);
  }
}
