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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for stat command.
 */
public final class StatCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void statFileNotExist() throws IOException {
    int ret = sFsShell.run("stat", "/NotExistFile");
    assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    assertEquals(-1, ret);
  }

  @Test
  public void statFileWildCard() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    sFsShell.run("stat", testDir + "/*");
    String res1 = mOutput.toString();
    assertTrue(res1.contains(testDir + "/foo"));
    assertTrue(res1.contains(testDir + "/bar"));
    assertTrue(res1.contains(testDir + "/foobar4"));
    assertFalse(res1.contains(testDir + "/foo/foobar1"));
    assertFalse(res1.contains(testDir + "/bar/foobar3"));

    sFsShell.run("stat", testDir + "/*/foo*");
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    assertTrue(res2.contains(testDir + "/foo/foobar1"));
    assertTrue(res2.contains(testDir + "/foo/foobar2"));
    assertTrue(res2.contains(testDir + "/bar/foobar3"));
    assertFalse(res2.contains(testDir + "/foobar4"));
  }

  @Test
  public void statDirectoryWildCard() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    sFsShell.run("stat", testDir + "/*");
    String res1 = mOutput.toString();
    assertTrue(res1.contains(testDir + "/foo"));
    assertTrue(res1.contains(testDir + "/bar"));
    assertTrue(res1.contains(testDir + "/foobar4"));
    assertFalse(res1.contains(testDir + "/foo/foobar1"));
    assertFalse(res1.contains(testDir + "/bar/foobar3"));

    sFsShell.run("stat", testDir + "/*/foo*");
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    assertTrue(res2.contains(testDir + "/foo/foobar1"));
    assertTrue(res2.contains(testDir + "/foo/foobar2"));
    assertTrue(res2.contains(testDir + "/bar/foobar3"));
    assertFalse(res2.contains(testDir + "/foobar4"));
  }

  @Test
  public void statFileFormat() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    String format = "%N-%z-%b-%u-%g-%Y";
    sFsShell.run("stat", "-f", format, testDir + "/foo/foobar1");
    String res1 = mOutput.toString();
    assertTrue(res1.startsWith("foobar1-10-1-"));

    format = "%N-%z-%b-%u-%g-%y";
    sFsShell.run("stat", "-f", format, testDir + "/foo/foobar1");
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    assertTrue(res2.startsWith("foobar1-10-1-"));
  }

  @Test
  public void statDirectoryFormat() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    String format = "%N-%z-%b-%Y-%u-%g";
    sFsShell.run("stat", "-f", format, testDir + "/foo");
    String res1 = mOutput.toString();
    assertTrue(res1.startsWith("foo-NA-NA-"));

    format = "%N-%z-%b-%y-%u-%g";
    sFsShell.run("stat", "-f", format, testDir + "/foo");
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    assertTrue(res2.startsWith("foo-NA-NA-"));
  }
}
