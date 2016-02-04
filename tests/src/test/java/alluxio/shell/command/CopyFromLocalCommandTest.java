/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import alluxio.TachyonURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.TachyonException;
import alluxio.shell.AbstractTfsShellTest;
import alluxio.shell.TfsShellUtilsTest;
import alluxio.util.io.BufferUtils;

/**
 * Tests for copyFromLocal command.
 */
public class CopyFromLocalCommandTest extends AbstractTfsShellTest {
  @Test
  public void copyDirectoryFromLocalAtomicTest() throws Exception {
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File testFile =
        generateFileContent("/localDir/testFile", BufferUtils.getIncreasingByteArray(10));
    File testDir = testFile.getParentFile();
    TachyonURI tachyonDirPath = new TachyonURI("/testDir");
    testFile.setReadable(false);

    String[] cmd = {"copyFromLocal", testDir.getPath(), tachyonDirPath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertFalse(mFileSystem.exists(tachyonDirPath));
    mOutput.reset();

    // If we put a copyable file in the directory, we should be able to copy just that file
    generateFileContent("/localDir/testFile2", BufferUtils.getIncreasingByteArray(20));
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertTrue(mFileSystem.exists(tachyonDirPath));
    Assert.assertTrue(mFileSystem.exists(new TachyonURI("/testDir/testFile2")));
    Assert.assertFalse(mFileSystem.exists(new TachyonURI("/testDir/testFile")));
  }

  @Test
  public void copyFromLocalAtomicTest() throws Exception {
    // copyFromLocal should not leave around any empty file metadata if it fails in the middle of
    // copying a file
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(10));
    TachyonURI tachyonFilePath = new TachyonURI("/testFile");
    // Set testFile1 to be not readable, so that when we try to open it, we fail. NOTE: for this to
    // test anything, we depend on the implementation of copyFromLocal creating the destination file
    // in Tachyon before it tries to open the source file
    testFile1.setReadable(false);
    String[] cmd = {"copyFromLocal", testFile1.getPath(), tachyonFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile1.getPath() + " (Permission denied)\n", mOutput.toString());

    // Make sure the alluxio file wasn't created anyways
    Assert.assertFalse(mFileSystem.exists(tachyonFilePath));
  }

  @Test
  public void copyFromLocalFileToDstPathTest() throws IOException, TachyonException {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.run("mkdir", "/dstDir");
    mFsShell.run("copyFromLocal", localFile.getPath(), "/dstDir");

    TachyonURI uri = new TachyonURI("/dstDir/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    byte[] read = readContent(uri, data.length);
    Assert.assertEquals(new String(read), dataString);
  }

  @Test
  public void copyFromLocalLargeTest() throws IOException, TachyonException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    mFsShell.run("copyFromLocal", testFile.getAbsolutePath(), "/testFile");
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(), "/testFile"}),
        mOutput.toString());
    TachyonURI uri = new TachyonURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    Assert.assertEquals(SIZE_BYTES, status.getLength());

    FileInStream tfis =
        mFileSystem.openFile(uri, OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE));
    byte[] read = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void copyFromLocalOverwriteTest() throws Exception {
    // This tests makes sure copyFromLocal will not overwrite an existing Tachyon file
    final int LEN1 = 10;
    final int LEN2 = 20;
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(LEN1));
    File testFile2 = generateFileContent("/testFile2", BufferUtils.getIncreasingByteArray(LEN2));
    TachyonURI tachyonFilePath = new TachyonURI("/testFile");

    // Write the first file
    String[] cmd1 = {"copyFromLocal", testFile1.getPath(), tachyonFilePath.getPath()};
    mFsShell.run(cmd1);
    Assert.assertEquals(getCommandOutput(cmd1), mOutput.toString());
    mOutput.reset();
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(tachyonFilePath, LEN1)));

    // Write the second file to the same location, which should cause an exception
    String[] cmd2 = {"copyFromLocal", testFile2.getPath(), tachyonFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd2));
    Assert.assertEquals(tachyonFilePath.getPath() + " already exists\n", mOutput.toString());
    // Make sure the original file is intact
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(tachyonFilePath, LEN1)));
  }

  @Test
  public void copyFromLocalTest() throws IOException, TachyonException {
    File testDir = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile =
        generateFileContent("/testDir/testFile", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        BufferUtils.getIncreasingByteArray(10, 20));

    mFsShell.run("copyFromLocal", testFile.getParent(), "/testDir");
    Assert.assertEquals(
        getCommandOutput(new String[]{"copyFromLocal", testFile.getParent(), "/testDir"}),
        mOutput.toString());
    TachyonURI uri1 = new TachyonURI("/testDir/testFile");
    TachyonURI uri2 = new TachyonURI("/testDir/testDirInner/testFile2");
    URIStatus status1 = mFileSystem.getStatus(uri1);
    URIStatus status2 = mFileSystem.getStatus(uri2);
    Assert.assertNotNull(status1);
    Assert.assertNotNull(status2);
    Assert.assertEquals(10, status1.getLength());
    Assert.assertEquals(20, status2.getLength());
    byte[] read = readContent(uri1, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
    read = readContent(uri2, 20);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, 20, read));
  }

  @Test
  public void copyFromLocalTestWithFullURI() throws IOException, TachyonException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String tachyonURI = "alluxio://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.run("copyFromLocal", testFile.getPath(), tachyonURI);
    String cmdOut =
        getCommandOutput(new String[]{"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    TachyonURI uri = new TachyonURI("/destFileURI");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(10L, status.getLength());
    byte[] read = readContent(uri, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyFromLocalWildcardExistingDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    mFileSystem.createDirectory(new TachyonURI("/testDir"));
    int ret = mFsShell.run("copyFromLocal",
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar1")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardHierTest() throws IOException {
    TfsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    int ret =
        mFsShell.run("copyFromLocal", mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*",
            "/testDir");

    mFsShell.run("ls", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foo/foobar1")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foo/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/bar/foobar3")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalWildcardNotDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mFileSystem);
    int ret =
        mFsShell.run("copyFromLocal", mLocalTachyonCluster.getTachyonHome()
            + "/testWildCards/*/foo*", "/testWildCards/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyFromLocalWildcardTest() throws IOException {
    TfsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    int ret =
        mFsShell.run("copyFromLocal", mLocalTachyonCluster.getTachyonHome()
            + "/testWildCards/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar1")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar3")));
    Assert.assertFalse(fileExist(new TachyonURI("/testDir/foobar4")));
  }
}
