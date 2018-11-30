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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.util.io.BufferUtils;

import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;

/**
 * Tests for cp command.
 */
public final class CpCommandIntegrationTest extends AbstractFileSystemShellTest {

  /**
   * Tests copying a file to a new location.
   */
  @Test
  public void copyFileNew() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", testDir + "/foobar4", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));

    Assert.assertTrue(equals(new AlluxioURI("/copy"), new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests copying a file to an existing directory.
   */
  @Test
  public void copyFileExisting() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", testDir + "/foobar4", testDir + "/bar");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + "/bar/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI(testDir + "/bar/foobar4"), new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests recursively copying a directory to a new location.
   */
  @Test
  public void copyDirNew() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "-R", testDir, "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/bar")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/bar/foobar3")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foo/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/bar/foobar3"), new AlluxioURI(testDir + "/bar/foobar3")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar1"), new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar2"), new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar4"), new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests recursively copying a directory to an existing directory.
   */
  @Test
  public void copyDirExisting() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "-R", testDir, testDir);
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir)));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir + "/bar")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir + "/bar/foobar3")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir + "/foo")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir + "/foo/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir + "/foo/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testDir + testDir + "/foobar4")));

    Assert.assertTrue(equals(new AlluxioURI(testDir + testDir + "/bar/foobar3"),
        new AlluxioURI(testDir + "/bar/foobar3")));
    Assert.assertTrue(equals(new AlluxioURI(testDir + testDir + "/foo/foobar1"),
        new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertTrue(equals(new AlluxioURI(testDir + testDir + "/foo/foobar2"),
        new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(equals(new AlluxioURI(testDir + testDir + "/foobar4"),
        new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests copying a list of files specified through a wildcard expression.
   */
  @Test
  public void copyWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", testDir + "/*/foo*", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar1")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar2")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/copy/foobar3")));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar1"), new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar2"), new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar3"), new AlluxioURI(testDir + "/bar/foobar3")));
  }

  /**
   * Tests invalid input arguments.
   */
  @Test
  public void copyInvalidArgs() throws Exception {
    FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
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

  @Test
  public void copyAfterWorkersNotAvailable() throws Exception {
    FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    File testFile = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    testFile.createNewFile();
    mFsShell.run("copyFromLocal", testFile.getPath(), "/");
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testFile")));
    mLocalAlluxioCluster.stopWorkers();
    mFsShell.run("cp", "/testFile", "/testFile2");
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/testFile2")));
  }

  private boolean equals(AlluxioURI file1, AlluxioURI file2) throws Exception {
    try (Closer closer = Closer.create()) {
      OpenFilePOptions openFileOptions = FileSystemClientOptions.getOpenFileOptions().toBuilder()
          .setReadType(ReadPType.READ_NO_CACHE).build();
      FileInStream is1 = closer.register(mFileSystem.openFile(file1, openFileOptions));
      FileInStream is2 = closer.register(mFileSystem.openFile(file2, openFileOptions));
      return IOUtils.contentEquals(is1, is2);
    }
  }

  @Test
  public void copyFromLocalFileToDstPath() throws Exception {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.run("mkdir", "/dstDir");
    mFsShell.run("cp", "file://" + localFile.getPath(), "/dstDir");

    AlluxioURI uri = new AlluxioURI("/dstDir/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    byte[] read = readContent(uri, data.length);
    Assert.assertEquals(new String(read), dataString);
  }

  @Test
  public void copyFromLocalDir() throws Exception {
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
    int ret = mFsShell.run("cp", "file://" + srcOuterDir.getPath() + "/", "/dstDir");
    Assert.assertEquals(0, ret);
    AlluxioURI dstURI1 = new AlluxioURI("/dstDir/srcFile1");
    AlluxioURI dstURI2 = new AlluxioURI("/dstDir/innerDir/srcFile2");
    AlluxioURI dstURI3 = new AlluxioURI("/dstDir/emptyDir");
    Assert.assertNotNull(mFileSystem.getStatus(dstURI1));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI2));
    Assert.assertNotNull(mFileSystem.getStatus(dstURI3));
  }

  @Test
  public void copyFromLocalDirToExistingFile() throws Exception {
    // Copy a directory from local to a file which exists in Alluxio filesystem. This case should
    // fail.
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    File innerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir/innerDir");
    localDir.mkdir();
    innerDir.mkdir();
    generateFileContent("/localDir/srcFile", BufferUtils.getIncreasingByteArray(10));
    mFileSystem.createFile(new AlluxioURI("/dstFile")).close();
    int ret = mFsShell.run("cp", "file://" + localDir.getPath(), "/dstFile");
    Assert.assertEquals(-1, ret);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/dstFile")).isFolder());
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/dstFile/innerDir")));
  }

  @Test
  public void copyFromLocalDirToExistingDir() throws Exception {
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
    int ret = mFsShell.run("cp", "file://" + srcOuterDir.getPath(), "/dstDir");
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
    int ret1 = mFsShell.run("cp", "file://" + srcOuterDir.getPath(), "/dstDir1");
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
  public void copyFromLocalLarge() throws Exception {
    File testFile = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    String[] cmd = new String[]{"cp", "file://" +  testFile.getAbsolutePath(), "/testFile"};
    mFsShell.run(cmd);
    Assert.assertEquals(getCommandOutput(cmd), mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    Assert.assertEquals(SIZE_BYTES, status.getLength());

    try (FileInStream tfis = mFileSystem.openFile(uri, FileSystemClientOptions.getOpenFileOptions()
        .toBuilder().setReadType(ReadPType.READ_NO_CACHE).build())) {
      byte[] read = new byte[SIZE_BYTES];
      tfis.read(read);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SIZE_BYTES, read));
    }
  }

  @Test
  public void copyFromLocalOverwrite() throws Exception {
    // This tests makes sure copyFromLocal will not overwrite an existing Alluxio file
    final int LEN1 = 10;
    final int LEN2 = 20;
    File testFile1 = generateFileContent("/testFile1", BufferUtils.getIncreasingByteArray(LEN1));
    File testFile2 = generateFileContent("/testFile2", BufferUtils.getIncreasingByteArray(LEN2));
    AlluxioURI alluxioFilePath = new AlluxioURI("/testFile");

    // Write the first file
    String[] cmd1 = {"cp", "file://" +  testFile1.getPath(), alluxioFilePath.getPath()};
    mFsShell.run(cmd1);
    Assert.assertEquals(getCommandOutput(cmd1), mOutput.toString());
    mOutput.reset();
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(alluxioFilePath, LEN1)));

    // Write the second file to the same location, which should cause an exception
    String[] cmd2 = {"cp", "file://" +  testFile2.getPath(), alluxioFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd2));
    Assert.assertEquals(alluxioFilePath.getPath() + " already exists\n", mOutput.toString());
    // Make sure the original file is intact
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(alluxioFilePath, LEN1)));
  }

  @Test
  public void copyFromLocal() throws Exception {
    File testDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile =
        generateFileContent("/testDir/testFile", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        BufferUtils.getIncreasingByteArray(10, 20));
    String[] cmd = new String[]{"cp", "file://" + testFile.getParent(), "/testDir"};
    mFsShell.run(cmd);
    Assert.assertEquals(getCommandOutput(cmd), mOutput.toString());
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
  public void copyFromLocalTestWithFullURI() throws Exception {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String alluxioURI = "alluxio://" + mLocalAlluxioCluster.getHostname() + ":"
        + mLocalAlluxioCluster.getMasterRpcPort() + "/destFileURI";
    String[] cmd = new String[]{"cp", "file://" + testFile.getPath(), alluxioURI};
    // when
    mFsShell.run(cmd);
    String cmdOut = getCommandOutput(cmd);
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/destFileURI");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(10L, status.getLength());
    byte[] read = readContent(uri, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyFromLocalWildcardExistingDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("cp", "file://" +  testDir + "/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardHier() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    int ret = mFsShell.run("cp", "file://" +  testDir + "/*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/bar/foobar3")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalWildcardNotDir() throws Exception {
    String localTestDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    String alluxioTestDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", "file://" +  localTestDir + "/*/foo*", alluxioTestDir
                           + "/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyFromLocalWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    int ret = mFsShell.run("cp", "file://" +  testDir + "/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
    Assert.assertFalse(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyToLocalDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", testDir,
            "file://" + mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocal() throws Exception {
    copyToLocalWithBytes(10);
  }

  @Test
  public void copyToLocalWildcardExistingDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);

    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();

    int ret = mFsShell.run("cp", testDir + "/*/foo*",
        "file://" + mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalWildcardHier() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell
        .run("cp", testDir + "/*", "file://" + mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocalWildcardNotDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile").createNewFile();

    int ret = mFsShell.run("cp", testDir + "/*/foo*",
        "file://" + mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyToLocalWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("cp", testDir + "/*/foo*",
        "file://" + mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Override
  protected void copyToLocalWithBytes(int bytes) throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WritePType.WRITE_MUST_CACHE,
        bytes);
    String[] cmd = new String[] {"cp", "/testFile",
        "file://" + mLocalAlluxioCluster.getAlluxioHome() + "/testFile"};
    mFsShell.run(cmd);
    Assert.assertEquals(getCommandOutput(cmd), mOutput.toString());
    fileReadTest("/testFile", 10);
  }
}
