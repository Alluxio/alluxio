/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for mv command.
 */
public final class MvCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void rename() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    sFsShell.run("mkdir", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertTrue(fileExists(new AlluxioURI("/testFolder1")));
    sFsShell.run("mv", "/testFolder1", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExists(new AlluxioURI("/testFolder")));
    Assert.assertFalse(fileExists(new AlluxioURI("/testFolder1")));
  }

  @Test
  public void renameParentDirectory() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    sFsShell.run("mkdir", "/test/File1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    sFsShell.run("mv", "/test", "/test2");
    toCompare.append(getCommandOutput(new String[] {"mv", "/test", "/test2"}));
    Assert.assertTrue(fileExists(new AlluxioURI("/test2/File1")));
    Assert.assertFalse(fileExists(new AlluxioURI("/test")));
    Assert.assertFalse(fileExists(new AlluxioURI("/test/File1")));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void renameToExistingFile() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    sFsShell.run("mkdir", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    sFsShell.run("mkdir", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    int ret = sFsShell.run("mv", "/testFolder1", "/testFolder");

    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    System.out.println(output);
    Assert.assertTrue(output.contains(
        "Cannot rename because destination already exists. src: /testFolder1 dst: /testFolder"));
  }
}
