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
 * Test for mkdir command with complex path
 */
public class MkdirComplexPathTest extends AbstractTfsShellTest {
  @Test
  public void mkdirComplexPathTest() throws IOException, TachyonException {
    mFsShell.run("mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File");
    TachyonFile tFile = mTfs.open(new TachyonURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir",
        "/Complex!@#$%^&*()-_=+[]{};\"'<>," + ".?/File"}), mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }
}
