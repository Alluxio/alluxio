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

import alluxio.AlluxioURI;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for mv command.
 */
public class MvCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void renameTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertTrue(fileExist(new AlluxioURI("/testFolder1")));
    mFsShell.run("mv", "/testFolder1", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExist(new AlluxioURI("/testFolder")));
    Assert.assertFalse(fileExist(new AlluxioURI("/testFolder1")));
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/test/File1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    mFsShell.run("mv", "/test", "/test2");
    toCompare.append(getCommandOutput(new String[] {"mv", "/test", "/test2"}));
    Assert.assertTrue(fileExist(new AlluxioURI("/test2/File1")));
    Assert.assertFalse(fileExist(new AlluxioURI("/test")));
    Assert.assertFalse(fileExist(new AlluxioURI("/test/File1")));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void renameToExistingFileTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.run("mkdir", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    int ret = mFsShell.run("mv", "/testFolder1", "/testFolder");

    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    System.out.println(output);
    Assert.assertTrue(output.contains("/testFolder already exists"));
  }
}
