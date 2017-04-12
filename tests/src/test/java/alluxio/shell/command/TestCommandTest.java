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
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for test command.
 */
public final class TestCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void testPathIsDirectoryWhenPathNotExist() throws IOException {
    int ret = mFsShell.run("test", "-d", "/testPath");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathIsDirectoryWhenPathIsDirectory() throws IOException, AlluxioException {
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("test", "-d", "/testDir");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testPathIsDirectoryWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("test", "-d", "/testFile");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathIsFileWhenPathNotExist() throws IOException {
    int ret = mFsShell.run("test", "-f", "/testPath");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathIsFileWhenPathIsDirectory() throws IOException, AlluxioException {
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("test", "-f", "/testDir");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathIsFileWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("test", "-f", "/testFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testPathExistWhenPathNotExist() throws IOException {
    int ret = mFsShell.run("test", "-e", "/testPath");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathExistWhenPathIsDirectory() throws IOException, AlluxioException {
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("test", "-e", "/testDir");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testPathExistWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("test", "-e", "/testFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testDirNotEmptyWhenPathNotExist() throws IOException {
    int ret = mFsShell.run("test", "-s", "/testPath");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testDirNotEmptyWhenPathIsDirectory() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testDir1/testFile1", WriteType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testDir2/testFile2", WriteType.MUST_CACHE, 10);
    String[] command = new String[] {"mkdir", "/testDir3/testDir4"};
    mFsShell.run(command);

    int ret = mFsShell.run("test", "-s", "/testDir1");
    Assert.assertEquals(0, ret);

    ret = mFsShell.run("test", "-s", "/testDir2");
    Assert.assertEquals(0, ret);

    ret = mFsShell.run("test", "-s", "/testDir3");
    Assert.assertEquals(0, ret);

    ret = mFsShell.run("test", "-s", "/testDir3/testDir4");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testDirNotEmptyWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile1", WriteType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile2", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("test", "-s", "/testFile1");
    Assert.assertEquals(1, ret);

    ret = mFsShell.run("test", "-s", "/testFile2");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testFileZeroLengthWhenPathNotExist() throws IOException {
    int ret = mFsShell.run("test", "-z", "/testPath");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testFileZeroLengthWhenPathIsDirectory() throws IOException, AlluxioException {
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("test", "-z", "/testDir");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testFileZeroLengthWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile1", WriteType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile2", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("test", "-z", "/testFile1");
    Assert.assertEquals(0, ret);

    ret = mFsShell.run("test", "-z", "/testFile2");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testWithoutOption() throws IOException, AlluxioException {
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("test", "/testDir");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testWrongOption() throws IOException, AlluxioException {
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("test", "-dfesz", "/testDir");
    Assert.assertEquals(1, ret);
  }
}
