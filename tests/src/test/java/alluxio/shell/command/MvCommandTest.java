/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
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
    Assert.assertTrue(fileExists(new AlluxioURI("/testFolder1")));
    mFsShell.run("mv", "/testFolder1", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExists(new AlluxioURI("/testFolder")));
    Assert.assertFalse(fileExists(new AlluxioURI("/testFolder1")));
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/test/File1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    mFsShell.run("mv", "/test", "/test2");
    toCompare.append(getCommandOutput(new String[] {"mv", "/test", "/test2"}));
    Assert.assertTrue(fileExists(new AlluxioURI("/test2/File1")));
    Assert.assertFalse(fileExists(new AlluxioURI("/test")));
    Assert.assertFalse(fileExists(new AlluxioURI("/test/File1")));
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
