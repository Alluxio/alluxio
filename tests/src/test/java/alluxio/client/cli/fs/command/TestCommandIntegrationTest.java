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
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.grpc.WritePType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for test command.
 */
public final class TestCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void testPathIsDirectoryWhenPathNotExist() throws Exception {
    int ret = sFsShell.run("test", "-d", "/testPath");
    Assert.assertEquals(3, ret);
  }

  @Test
  public void testPathIsDirectoryWhenPathIsDirectory() throws Exception {
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("test", "-d", "/testDir");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testPathIsDirectoryWhenPathIsFile() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("test", "-d", "/testFile");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathIsFileWhenPathNotExist() throws Exception {
    int ret = sFsShell.run("test", "-f", "/testPath");
    Assert.assertEquals(3, ret);
  }

  @Test
  public void testPathIsFileWhenPathIsDirectory() throws Exception {
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("test", "-f", "/testDir");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testPathIsFileWhenPathIsFile() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("test", "-f", "/testFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testPathExistWhenPathNotExist() throws Exception {
    int ret = sFsShell.run("test", "-e", "/testPath");
    Assert.assertEquals(3, ret);
  }

  @Test
  public void testPathExistWhenPathIsDirectory() throws Exception {
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("test", "-e", "/testDir");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testPathExistWhenPathIsFile() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("test", "-e", "/testFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void testDirNotEmptyWhenPathNotExist() throws Exception {
    int ret = sFsShell.run("test", "-s", "/testPath");
    Assert.assertEquals(3, ret);
  }

  @Test
  public void testDirNotEmptyWhenPathIsDirectory() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testDir1/testFile1", WritePType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testDir2/testFile2", WritePType.MUST_CACHE, 10);
    String[] command = new String[] {"mkdir", "/testDir3/testDir4"};
    sFsShell.run(command);

    int ret = sFsShell.run("test", "-s", "/testDir1");
    Assert.assertEquals(0, ret);

    ret = sFsShell.run("test", "-s", "/testDir2");
    Assert.assertEquals(0, ret);

    ret = sFsShell.run("test", "-s", "/testDir3");
    Assert.assertEquals(0, ret);

    ret = sFsShell.run("test", "-s", "/testDir3/testDir4");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testDirNotEmptyWhenPathIsFile() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile1", WritePType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile2", WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("test", "-s", "/testFile1");
    Assert.assertEquals(1, ret);

    ret = sFsShell.run("test", "-s", "/testFile2");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testFileZeroLengthWhenPathNotExist() throws Exception {
    int ret = sFsShell.run("test", "-z", "/testPath");
    Assert.assertEquals(3, ret);
  }

  @Test
  public void testFileZeroLengthWhenPathIsDirectory() throws Exception {
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("test", "-z", "/testDir");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testFileZeroLengthWhenPathIsFile() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile1", WritePType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile2", WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("test", "-z", "/testFile1");
    Assert.assertEquals(0, ret);

    ret = sFsShell.run("test", "-z", "/testFile2");
    Assert.assertEquals(1, ret);
  }

  @Test
  public void testWithoutOption() throws Exception {
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("test", "/testDir");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void testWrongOption() throws Exception {
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("test", "-dfesz", "/testDir");
    Assert.assertEquals(-1, ret);
  }
}
