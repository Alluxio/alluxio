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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for cp command.
 */
public class CpCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void copyDirTest() throws Exception {
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

  @Test
  public void copyWildcardNotDirTest() throws Exception {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret;
    ret = mFsShell.run("cp", "/testWildCards", "/testWildCards/foobar4");
    Assert.assertEquals(-1, ret);
    ret = mFsShell.run("cp", "/testWildCards/*", "/testWildCards/foobar4");
    Assert.assertEquals(-1, ret);
  }

  private boolean equals(AlluxioURI file1, AlluxioURI file2) throws Exception {
    URIStatus status1;
    try {
      status1 = mFileSystem.getStatus(file1);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    URIStatus status2;
    try {
      status2 = mFileSystem.getStatus(file2);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    if (status1.getLength() != status2.getLength()) {
      return false;
    }

    if (status1.getInMemoryPercentage() != status2.getInMemoryPercentage()) {
      return false;
    }

    if (status1.isPersisted() != status2.isPersisted()) {
      return false;
    }

    try (Closer closer = Closer.create()) {
      OpenFileOptions openFileOptions = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is1 = closer.register(mFileSystem.openFile(file1, openFileOptions));
      FileInStream is2 = closer.register(mFileSystem.openFile(file2, openFileOptions));
      IOUtils.contentEquals(is1, is2);
    }

    return true;
  }
}
