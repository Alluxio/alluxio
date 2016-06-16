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
   * Tests recursively copying a directory to a new directory.
   */
  @Test
  public void copyDirNewTest() throws Exception {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "/testWildCards", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/bar")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/bar/foobar3")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/bar/foobar3"), new AlluxioURI("/testWildCards/bar/foobar3")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar1"), new AlluxioURI("/testWildCards/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar2"), new AlluxioURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar4"), new AlluxioURI("/testWildCards/foobar4")));
  }

  /**
   * Tests recursively copying a directory to an existing directory.
   */
  @Test
  public void copyDirExistingTest() throws Exception {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "/testWildCards", "/testWildCards");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards/bar")));
    Assert
        .assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards/bar/foobar3")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards/foo")));
    Assert
        .assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards/foo/foobar1")));
    Assert
        .assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards/foo/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testWildCards/testWildCards/foobar4")));

    Assert.assertTrue(equals(new AlluxioURI("/testWildCards/testWildCards/bar/foobar3"),
        new AlluxioURI("/testWildCards/bar/foobar3")));
    Assert.assertTrue(equals(new AlluxioURI("/testWildCards/testWildCards/foo/foobar1"),
        new AlluxioURI("/testWildCards/foo/foobar1")));
    Assert.assertTrue(equals(new AlluxioURI("/testWildCards/testWildCards/foo/foobar2"),
        new AlluxioURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(equals(new AlluxioURI("/testWildCards/testWildCards/foobar4"),
        new AlluxioURI("/testWildCards/foobar4")));
  }

  /**
   * Tests copying a list of files specified through a wildcard expression.
   */
  @Test
  public void copyWildcardDirTest() throws Exception {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "/testWildCards/*/foo*", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar3")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar1"), new AlluxioURI("/testWildCards/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar2"), new AlluxioURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar3"), new AlluxioURI("/testWildCards/bar/foobar3")));
  }

  /**
   * Tests invalid input arguments.
   */
  @Test
  public void copyInvalidArgsTest() throws Exception {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret;
    // cannot copy a directory onto an existing file
    ret = mFsShell.run("cp", "/testWildCards", "/testWildCards/foobar4");
    Assert.assertEquals(-1, ret);
    // cannot copy list of file onto a existing file
    ret = mFsShell.run("cp", "/testWildCards/*", "/testWildCards/foobar4");
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
