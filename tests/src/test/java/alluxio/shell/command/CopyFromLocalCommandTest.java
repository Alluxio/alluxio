/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Tests for copyFromLocal command.
 */
public class CopyFromLocalCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void copyDirectoryFromLocalAtomicTest() throws Exception {
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    localDir.mkdir();
    File testFile =
        generateFileContent("/localDir/testFile", BufferUtils.getIncreasingByteArray(10));
    File testDir = testFile.getParentFile();
    AlluxioURI alluxioDirPath = new AlluxioURI("/testDir");
    testFile.setReadable(false);

    String[] cmd = {"copyFromLocal", testDir.getPath(), alluxioDirPath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertFalse(mFileSystem.exists(alluxioDirPath));
    mOutput.reset();

    // If we put a copyable file in the directory, we should be able to copy just that file
    generateFileContent("/localDir/testFile2", BufferUtils.getIncreasingByteArray(20));
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertTrue(mFileSystem.exists(alluxioDirPath));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testDir/testFile2")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/testFile")));

    // The directory should also be deleted from Alluxio filesystem when all files in the
    // directory are failed.
    File innerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir/innerDir");
    innerDir.mkdir();
    File innerFile = generateFileContent("/localDir/innerDir/innerFile1",
            BufferUtils.getIncreasingByteArray(30));
    innerFile.setReadable(false);
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertTrue(mFileSystem.exists(alluxioDirPath));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testDir/testFile2")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/testFile")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/innerDir")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/innerDir/innerFile1")));
  }

  @Test
  public void copyDirectoryFromLocalToExistingDirAtomicTest() throws Exception {
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    localDir.mkdir();
    File testFile =
            generateFileContent("/localDir/testFile", BufferUtils.getIncreasingByteArray(10));
    File testDir = testFile.getParentFile();
    AlluxioURI alluxioDirPath = new AlluxioURI("/testDir");
    // Create the destination directory before call command of 'copyFromLocal'.
    mFileSystem.createDirectory(alluxioDirPath);
    testFile.setReadable(false);

    String[] cmd = {"copyFromLocal", testDir.getPath(), alluxioDirPath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    // The destination directory should not be deleted.
    Assert.assertTrue(mFileSystem.exists(alluxioDirPath));
    mOutput.reset();

    // If we put a copyable file in the directory, we should be able to copy just that file
    generateFileContent("/localDir/testFile2", BufferUtils.getIncreasingByteArray(20));
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertTrue(mFileSystem.exists(alluxioDirPath));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testDir/testFile2")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/testFile")));

    // The directory should also be deleted from Alluxio filesystem when all files in the
    // directory are failed.
    File innerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir/innerDir");
    innerDir.mkdir();
    File innerFile = generateFileContent("/localDir/innerDir/innerFile1",
            BufferUtils.getIncreasingByteArray(30));
    innerFile.setReadable(false);
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertTrue(mFileSystem.exists(alluxioDirPath));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testDir/testFile2")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/testFile")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/innerDir")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testDir/innerDir/innerFile1")));
  }

  @Test
  public void copyFromLocalAtomicTest() throws Exception {
    // copyFromLocal should not leave around any empty file metadata if it fails in the middle of
    // copying a file
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(10));
    AlluxioURI alluxioFilePath = new AlluxioURI("/testFile");
    // Set testFile1 to be not readable, so that when we try to open it, we fail. NOTE: for this to
    // test anything, we depend on the implementation of copyFromLocal creating the destination file
    // in Alluxio before it tries to open the source file
    testFile1.setReadable(false);
    String[] cmd = {"copyFromLocal", testFile1.getPath(), alluxioFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile1.getPath() + " (Permission denied)\n", mOutput.toString());

    // Make sure the alluxio file wasn't created anyways
    Assert.assertFalse(mFileSystem.exists(alluxioFilePath));
  }

  @Test
  public void copyFromLocalFileToDstPathTest() throws IOException, AlluxioException {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.run("mkdir", "/dstDir");
    mFsShell.run("copyFromLocal", localFile.getPath(), "/dstDir");

    AlluxioURI uri = new AlluxioURI("/dstDir/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    byte[] read = readContent(uri, data.length);
    Assert.assertEquals(new String(read), dataString);
  }

  @Test
  public void copyFromLocalDirTest() throws IOException, AlluxioException {
    // Copy a directory from local to Alluxio filesystem, which the destination uri was not created
    // before.
    File srcOuterDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/outerDir");
    File srcInnerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/outerDir/innerDir");
    File emptyDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/outerDir/emptyDir");
    srcOuterDir.mkdir();
    srcInnerDir.mkdir();
    emptyDir.mkdir();
    generateFileContent("/outerDir/srcFile1", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/outerDir/innerDir/srcFile2", BufferUtils.getIncreasingByteArray(10));
    int ret = mFsShell.run("copyFromLocal", srcOuterDir.getPath() + "/", "/dstDir");
    Assert.assertEquals(0, ret);
    AlluxioURI dstURI1 = new AlluxioURI("/dstDir/srcFile1");
    AlluxioURI dstURI2 = new AlluxioURI("/dstDir/innerDir/srcFile2");
    AlluxioURI dstURI3 = new AlluxioURI("/dstDir/emptyDir");
    Assert.assertNotNull(mFileSystem.getStatus(dstURI1));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI2));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI3));
  }

  @Test
  public void copyFromLocalDirToExistingFileTest() throws IOException, AlluxioException {
    // Copy a directory from local to a file which exists in Alluxio filesystem. This case should
    // fail.
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    File innerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir/innerDir");
    localDir.mkdir();
    innerDir.mkdir();
    generateFileContent("/localDir/srcFile", BufferUtils.getIncreasingByteArray(10));
    mFileSystem.createFile(new AlluxioURI("/dstFile"));
    int ret = mFsShell.run("copyFromLocal", localDir.getPath(), "/dstFile");
    Assert.assertEquals(-1, ret);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/dstFile")).isFolder());
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/dstFile/innerDir")));
  }

  @Test
  public void copyFromLocalDirToExistingDirTest() throws IOException, AlluxioException {
    // Copy a directory from local to Alluxio filesystem, which the destination uri has been
    // created before.
    File srcOuterDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/outerDir");
    File srcInnerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/outerDir/innerDir");
    File emptyDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/outerDir/emptyDir");
    srcOuterDir.mkdir();
    srcInnerDir.mkdir();
    emptyDir.mkdir();
    generateFileContent("/outerDir/srcFile1", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/outerDir/innerDir/srcFile2", BufferUtils.getIncreasingByteArray(10));
    // Copying a directory to a destination directory which exists and doesn't contain the copied
    // directory.
    mFileSystem.createDirectory(new AlluxioURI("/dstDir"));
    int ret = mFsShell.run("copyFromLocal", srcOuterDir.getPath(), "/dstDir");
    Assert.assertEquals(0, ret);
    AlluxioURI dstURI1 = new AlluxioURI("/dstDir/srcFile1");
    AlluxioURI dstURI2 = new AlluxioURI("/dstDir/innerDir/srcFile2");
    AlluxioURI dstURI3 = new AlluxioURI("/dstDir/emptyDir");
    Assert.assertNotNull(mFileSystem.getStatus(dstURI1));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI2));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI3));

    // Copying a directory to a destination directory which exists and does contain the copied
    // directory.
    mFileSystem.createDirectory(new AlluxioURI("/dstDir1"));
    mFileSystem.createDirectory(new AlluxioURI("/dstDir1/innerDir"));
    int ret1 = mFsShell.run("copyFromLocal", srcOuterDir.getPath(), "/dstDir1");
    Assert.assertEquals(-1, ret1);
    dstURI1 = new AlluxioURI("/dstDir1/srcFile1");
    dstURI2 = new AlluxioURI("/dstDir1/innerDir/srcFile2");
    dstURI3 = new AlluxioURI("/dstDir1/emptyDir");
    Assert.assertNotNull(mFileSystem.getStatus(dstURI1));
    // The directory already exists. But the sub directory shouldn't be copied.
    Assert.assertFalse(mFileSystem.exists(dstURI2));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI3));
  }

  @Test
  public void copyFromLocalLargeTest() throws IOException, AlluxioException {
    File testFile = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    mFsShell.run("copyFromLocal", testFile.getAbsolutePath(), "/testFile");
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(), "/testFile"}),
        mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/testFile");
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
    // This tests makes sure copyFromLocal will not overwrite an existing Alluxio file
    final int LEN1 = 10;
    final int LEN2 = 20;
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(LEN1));
    File testFile2 = generateFileContent("/testFile2", BufferUtils.getIncreasingByteArray(LEN2));
    AlluxioURI alluxioFilePath = new AlluxioURI("/testFile");

    // Write the first file
    String[] cmd1 = {"copyFromLocal", testFile1.getPath(), alluxioFilePath.getPath()};
    mFsShell.run(cmd1);
    Assert.assertEquals(getCommandOutput(cmd1), mOutput.toString());
    mOutput.reset();
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(alluxioFilePath, LEN1)));

    // Write the second file to the same location, which should cause an exception
    String[] cmd2 = {"copyFromLocal", testFile2.getPath(), alluxioFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd2));
    Assert.assertEquals(alluxioFilePath.getPath() + " already exists\n", mOutput.toString());
    // Make sure the original file is intact
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(alluxioFilePath, LEN1)));
  }

  @Test
  public void copyFromLocalTest() throws IOException, AlluxioException {
    File testDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile =
        generateFileContent("/testDir/testFile", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        BufferUtils.getIncreasingByteArray(10, 20));

    mFsShell.run("copyFromLocal", testFile.getParent(), "/testDir");
    Assert.assertEquals(
        getCommandOutput(new String[]{"copyFromLocal", testFile.getParent(), "/testDir"}),
        mOutput.toString());
    AlluxioURI uri1 = new AlluxioURI("/testDir/testFile");
    AlluxioURI uri2 = new AlluxioURI("/testDir/testDirInner/testFile2");
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
  public void copyFromLocalTestWithFullURI() throws IOException, AlluxioException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String alluxioURI = "alluxio://" + mLocalAlluxioCluster.getMasterHostname() + ":"
        + mLocalAlluxioCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.run("copyFromLocal", testFile.getPath(), alluxioURI);
    String cmdOut =
        getCommandOutput(new String[]{"copyFromLocal", testFile.getPath(), alluxioURI});
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/destFileURI");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(10L, status.getLength());
    byte[] read = readContent(uri, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyFromLocalWildcardExistingDirTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("copyFromLocal",
        mLocalAlluxioCluster.getAlluxioHome() + "/testWildCards/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardHierTest() throws IOException {
    AlluxioShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    int ret =
        mFsShell.run("copyFromLocal", mLocalAlluxioCluster.getAlluxioHome() + "/testWildCards/*",
            "/testDir");

    mFsShell.run("ls", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/bar/foobar3")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalWildcardNotDirTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret =
        mFsShell.run("copyFromLocal", mLocalAlluxioCluster.getAlluxioHome()
            + "/testWildCards/*/foo*", "/testWildCards/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyFromLocalWildcardTest() throws IOException {
    AlluxioShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    int ret =
        mFsShell.run("copyFromLocal", mLocalAlluxioCluster.getAlluxioHome()
            + "/testWildCards/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
    Assert.assertFalse(fileExists(new AlluxioURI("/testDir/foobar4")));
  }
}
