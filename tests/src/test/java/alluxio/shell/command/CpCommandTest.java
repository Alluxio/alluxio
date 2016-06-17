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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for cp command.
 */
public final class CpCommandTest extends AbstractAlluxioShellTest {

  /**
   * Tests copying a file to a new location.
   */
  @Test
  public void copyFileNewTest() throws Exception {
    String rootDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", rootDir + "/foobar4", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));

    Assert.assertTrue(equals(new AlluxioURI("/copy"), new AlluxioURI(rootDir + "/foobar4")));
  }

  /**
   * Tests copying a file to an existing directory.
   */
  @Test
  public void copyFileExistingTest() throws Exception {
    String rootDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", rootDir + "/foobar4", rootDir + "/bar");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + "/bar/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI(rootDir + "/bar/foobar4"), new AlluxioURI(rootDir + "/foobar4")));
  }

  /**
   * Tests recursively copying a directory to a new location.
   */
  @Test
  public void copyDirNewTest() throws Exception {
    String rootDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "-R", rootDir, "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/bar")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/bar/foobar3")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/bar/foobar3"), new AlluxioURI(rootDir + "/bar/foobar3")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar1"), new AlluxioURI(rootDir + "/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar2"), new AlluxioURI(rootDir + "/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar4"), new AlluxioURI(rootDir + "/foobar4")));
  }

  /**
   * Tests recursively copying a directory to an existing directory.
   */
  @Test
  public void copyDirExistingTest() throws Exception {
    String rootDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "-R", rootDir, rootDir);
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir)));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir + "/bar")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir + "/bar/foobar3")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir + "/foo")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir + "/foo/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir + "/foo/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(rootDir + rootDir + "/foobar4")));

    Assert.assertTrue(equals(new AlluxioURI(rootDir + rootDir + "/bar/foobar3"),
        new AlluxioURI(rootDir + "/bar/foobar3")));
    Assert.assertTrue(equals(new AlluxioURI(rootDir + rootDir + "/foo/foobar1"),
        new AlluxioURI(rootDir + "/foo/foobar1")));
    Assert.assertTrue(equals(new AlluxioURI(rootDir + rootDir + "/foo/foobar2"),
        new AlluxioURI(rootDir + "/foo/foobar2")));
    Assert.assertTrue(equals(new AlluxioURI(rootDir + rootDir + "/foobar4"),
        new AlluxioURI(rootDir + "/foobar4")));
  }

  /**
   * Tests copying a list of files specified through a wildcard expression.
   */
  @Test
  public void copyWildcardTest() throws Exception {
    String rootDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", rootDir + "/*/foo*", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar3")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar1"), new AlluxioURI(rootDir + "/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar2"), new AlluxioURI(rootDir + "/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar3"), new AlluxioURI(rootDir + "/bar/foobar3")));
  }

  /**
   * Tests invalid input arguments.
   */
  @Test
  public void copyInvalidArgsTest() throws Exception {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret;
    // cannot copy a directory without -R
    ret = mFsShell.run("cp", "/testDir", "/copy");
    Assert.assertEquals(-1, ret);
    // cannot copy a directory onto an existing file
    ret = mFsShell.run("cp", "-R", "/testDir", "/testDir/foobar4");
    Assert.assertEquals(-1, ret);
    // cannot copy list of file onto a existing file
    ret = mFsShell.run("cp", "-R", "/testDir/*", "/testDir/foobar4");
    Assert.assertEquals(-1, ret);
  }

  private boolean equals(AlluxioURI file1, AlluxioURI file2) throws Exception {
    try (Closer closer = Closer.create()) {
      OpenFileOptions openFileOptions = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is1 = closer.register(mFileSystem.openFile(file1, openFileOptions));
      FileInStream is2 = closer.register(mFileSystem.openFile(file2, openFileOptions));
      return IOUtils.contentEquals(is1, is2);
    }
  }
}
