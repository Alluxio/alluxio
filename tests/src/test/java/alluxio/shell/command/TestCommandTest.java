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

import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
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
    mFsShell.run("test", "-d", "/testPath");
    Assert.assertEquals("1\n", mOutput.toString());
  }

  @Test
  public void testPathIsDirectoryWhenPathIsDirectory() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    mFsShell.run("test", "-d", "/testDir");
    String expected = getCommandOutput(command);
    expected += "0\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void testPathIsDirectoryWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("test", "-d", "/testFile");
    Assert.assertEquals("1\n", mOutput.toString());
  }

  @Test
  public void testPathIsFileWhenPathNotExist() throws IOException {
    mFsShell.run("test", "-f", "/testPath");
    Assert.assertEquals("1\n", mOutput.toString());
  }

  @Test
  public void testPathIsFileWhenPathIsDirectory() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    mFsShell.run("test", "-f", "/testDir");
    String expected = getCommandOutput(command);
    expected += "1\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void testPathIsFileWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("test", "-f", "/testFile");
    Assert.assertEquals("0\n", mOutput.toString());
  }

  @Test
  public void testPathExistWhenPathNotExist() throws IOException {
    mFsShell.run("test", "-e", "/testPath");
    Assert.assertEquals("1\n", mOutput.toString());
  }

  @Test
  public void testPathExistWhenPathIsDirectory() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    mFsShell.run("test", "-e", "/testDir");
    String expected = getCommandOutput(command);
    expected += "0\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void testPathExistWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("test", "-e", "/testFile");
    Assert.assertEquals("0\n", mOutput.toString());
  }

  @Test
  public void testDirNotEmptyWhenPathNotExist() throws IOException {
    mFsShell.run("test", "-s", "/testPath");
    Assert.assertEquals("1\n", mOutput.toString());
  }

  @Test
  public void testDirNotEmptyWhenPathIsDirectory() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir1/testDir2"};
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testDir3/testFile1", WriteType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testDir4/testFile2", WriteType.MUST_CACHE, 10);
    mFsShell.run(command);
    String expected = getCommandOutput(command);
    mFsShell.run("test", "-s", "/testDir1");
    expected += "0\n";
    Assert.assertEquals(expected, mOutput.toString());

    mFsShell.run("test", "-s", "/testDir1/testDir2");
    expected += "1\n";
    Assert.assertEquals(expected, mOutput.toString());

    mFsShell.run("test", "-s", "/testDir3");
    expected += "0\n";
    Assert.assertEquals(expected, mOutput.toString());

    mFsShell.run("test", "-s", "/testDir4");
    expected += "0\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void testDirNotEmptyWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile1", WriteType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile2", WriteType.MUST_CACHE, 10);
    String expected = "";
    mFsShell.run("test", "-s", "/testFile1");
    expected += "1\n";
    Assert.assertEquals(expected, mOutput.toString());

    mFsShell.run("test", "-s", "/testFile2");
    expected += "1\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void testFileZeroLengthWhenPathNotExist() throws IOException {
    mFsShell.run("test", "-z", "/testPath");
    Assert.assertEquals("1\n", mOutput.toString());
  }

  @Test
  public void testFileZeroLengthWhenPathIsDirectory() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    mFsShell.run("test", "-z", "/testDir");
    String expected = getCommandOutput(command);
    expected += "1\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void testFileZeroLengthWhenPathIsFile() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile1", WriteType.MUST_CACHE, 0);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testFile2", WriteType.MUST_CACHE, 10);
    String expected = "";
    mFsShell.run("test", "-z", "/testFile1");
    expected += "0\n";
    Assert.assertEquals(expected, mOutput.toString());

    mFsShell.run("test", "-z", "/testFile2");
    expected += "1\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
