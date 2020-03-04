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
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for rm command.
 */
public final class RmCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void rmNotExistingDir() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    sFsShell.run("mkdir", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    sFsShell.run("rm", "/testFolder");
    toCompare.append("/testFolder is a directory, to remove it, please use \"rm -R <path>\"\n");
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void rmNotExistingFile() throws IOException {
    sFsShell.run("rm", "/testFile");
    String expected = ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testFile") + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void rm() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    sFsShell.run("mkdir", "/testFolder1/testFolder2");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    sFsShell.run("touch", "/testFolder1/testFolder2/testFile2");
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    AlluxioURI testFolder1 = new AlluxioURI("/testFolder1");
    AlluxioURI testFolder2 = new AlluxioURI("/testFolder1/testFolder2");
    AlluxioURI testFile2 = new AlluxioURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(fileExists(testFolder1));
    Assert.assertTrue(fileExists(testFolder2));
    Assert.assertTrue(fileExists(testFile2));
    sFsShell.run("rm", "/testFolder1/testFolder2/testFile2");
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExists(testFolder1));
    Assert.assertTrue(fileExists(testFolder2));
    Assert.assertFalse(fileExists(testFile2));
  }

  @Test
  public void rmWildCard() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    sFsShell.run("rm", testDir + "/foo/foo*");
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/bar/foobar3")));

    sFsShell.run("rm", testDir + "/*");
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foobar4")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/foo")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/bar")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/bar/foobar3")));
  }

  @Test
  public void rmr() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    sFsShell.run("mkdir", "/testFolder1/testFolder2");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    sFsShell.run("touch", "/testFolder1/testFolder2/testFile2");
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    AlluxioURI testFolder1 = new AlluxioURI("/testFolder1");
    AlluxioURI testFolder2 = new AlluxioURI("/testFolder1/testFolder2");
    AlluxioURI testFile2 = new AlluxioURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(fileExists(testFolder1));
    Assert.assertTrue(fileExists(testFolder2));
    Assert.assertTrue(fileExists(testFile2));
    sFsShell.run("rm", "-R", "/testFolder1/testFolder2/testFile2");
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExists(testFolder1));
    Assert.assertTrue(fileExists(testFolder2));
    Assert.assertFalse(fileExists(testFile2));
    sFsShell.run("rm", "-R", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"rmr", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertFalse(fileExists(testFolder1));
    Assert.assertFalse(fileExists(testFolder2));
    Assert.assertFalse(fileExists(testFile2));
  }

  @Test
  public void rmrWildCard() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    sFsShell.run("rm",  "-R", testDir + "/foo/foo*");
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/foo")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/bar/foobar3")));

    sFsShell.run("rm", "-R", testDir + "/ba*");
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/bar")));
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/bar/foobar3")));
    Assert.assertTrue(fileExists(new AlluxioURI(testDir + "/foobar4")));

    sFsShell.run("rm", "-R", testDir + "/*");
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/bar")));
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foo")));
    Assert.assertFalse(fileExists(new AlluxioURI(testDir + "/foobar4")));
  }
}
