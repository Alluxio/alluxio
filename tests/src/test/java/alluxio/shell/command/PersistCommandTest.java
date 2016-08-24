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
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.ExceptionMessage;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for persist command.
 */
public final class PersistCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void persist() throws Exception {
    String testFilePath = "/testPersist/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFilePath, WriteType.MUST_CACHE, 10);
    Assert
        .assertFalse(mFileSystem.getStatus(new AlluxioURI("/testPersist/testFile")).isPersisted());

    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n", mOutput.toString());
    checkFilePersisted(new AlluxioURI("/testPersist/testFile"), 10);
  }

  @Test
  public void persistDirectory() throws Exception {
    // Set the default write type to MUST_CACHE, so that directories are not persisted by default
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    Assert
        .assertFalse(mFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    Assert
        .assertFalse(mFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
    int ret = mFsShell.run("persist", testDir);
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    Assert
        .assertTrue(mFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    Assert
        .assertTrue(mFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
    checkFilePersisted(new AlluxioURI(testDir + "/foo/foobar1"), 10);
    checkFilePersisted(new AlluxioURI(testDir + "/foo/foobar2"), 20);
    checkFilePersisted(new AlluxioURI(testDir + "/bar/foobar3"), 30);
    checkFilePersisted(new AlluxioURI(testDir + "/foobar4"), 40);
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void persistMultiFiles() throws Exception {
    String filePath1 = "/testPersist/testFile1";
    String filePath2 = "/testFile2";
    String filePath3 = "/testPersist/testFile3";
    FileSystemTestUtils.createByteFile(mFileSystem, filePath1, WriteType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(mFileSystem, filePath2, WriteType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(mFileSystem, filePath3, WriteType.MUST_CACHE, 30);

    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(filePath1)).isPersisted());
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(filePath2)).isPersisted());
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(filePath3)).isPersisted());

    int ret = mFsShell.run("persist", filePath1, filePath2, filePath3);
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath1), 10);
    checkFilePersisted(new AlluxioURI(filePath2), 20);
    checkFilePersisted(new AlluxioURI(filePath3), 30);
  }

  /**
   * Tests persisting files and directories together in one persist command.
   */
  @Test
  public void persistMultiFilesAndDirs() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    Assert.assertFalse(
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).isPersisted());
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());

    int ret = mFsShell.run("persist", testDir + "/foo/foobar1", testDir + "/foobar4",
        testDir + "/bar", testDir + "/bar/foobar3");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    Assert.assertTrue(mFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    Assert.assertFalse(
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).isPersisted());
    Assert.assertTrue(mFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
    checkFilePersisted(new AlluxioURI(testDir + "/foo/foobar1"), 10);
    checkFilePersisted(new AlluxioURI(testDir + "/bar/foobar3"), 30);
    checkFilePersisted(new AlluxioURI(testDir + "/foobar4"), 40);
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void persistNonexistentFile() throws Exception {
    // Cannot persist a nonexistent file
    String path = "/testPersistNonexistent";
    int ret = mFsShell.run("persist", path);
    Assert.assertEquals(-1, ret);
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path) + "\n",
        mOutput.toString());
  }

  @Test
  public void persistTwice() throws Exception {
    // Persisting an already-persisted file is okay
    String testFilePath = "/testPersist/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFilePath, WriteType.MUST_CACHE, 10);
    Assert
        .assertFalse(mFileSystem.getStatus(new AlluxioURI("/testPersist/testFile")).isPersisted());
    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n" + testFilePath
        + " is already persisted\n", mOutput.toString());
    checkFilePersisted(new AlluxioURI("/testPersist/testFile"), 10);
  }
}
