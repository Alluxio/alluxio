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
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.SystemPropertyRule;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.util.io.BufferUtils;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

/**
 * Tests for copyFromLocal command.
 */
public final class CopyFromLocalCommandIntegrationTest extends AbstractFileSystemShellTest {
  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void copyFromLocalFileToDstPath() throws IOException, AlluxioException {
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
  public void copyFromLocalDir() throws IOException, AlluxioException {
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
  public void copyFromLocalDirToExistingFile() throws IOException, AlluxioException {
    // Copy a directory from local to a file which exists in Alluxio filesystem. This case should
    // fail.
    File localDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    File innerDir = new File(mLocalAlluxioCluster.getAlluxioHome() + "/localDir/innerDir");
    localDir.mkdir();
    innerDir.mkdir();
    generateFileContent("/localDir/srcFile", BufferUtils.getIncreasingByteArray(10));
    mFileSystem.createFile(new AlluxioURI("/dstFile")).close();
    int ret = mFsShell.run("copyFromLocal", localDir.getPath(), "/dstFile");
    Assert.assertEquals(-1, ret);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/dstFile")).isFolder());
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/dstFile/innerDir")));
  }

  @Test
  public void copyFromLocalDirToExistingDir() throws IOException, AlluxioException {
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
  public void copyFromLocalLarge() throws IOException, AlluxioException {
    File testFile = new File(mLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    mFsShell.run("copyFromLocal", testFile.getAbsolutePath(), "/testFile");
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(),
            "/testFile"}),
        mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    Assert.assertEquals(SIZE_BYTES, status.getLength());

    try (FileInStream tfis =
        mFileSystem.openFile(uri, OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE))) {
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
  public void copyFromLocal() throws IOException, AlluxioException {
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
  public void copyFromLocalMustCacheThenCacheThrough() throws Exception {
    File file = mTestFolder.newFile();
    try (Closeable c = new ConfigurationRule(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT,
        WriteType.MUST_CACHE.toString()).toResource()) {
      Assert.assertEquals(0, mFsShell.run("copyFromLocal", file.getAbsolutePath(), "/"));
    }
    try (Closeable c = new ConfigurationRule(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT,
        WriteType.CACHE_THROUGH.toString()).toResource()) {
      mOutput.reset();
      mFsShell.run("copyFromLocal", file.getAbsolutePath(), "/");
    }
    Assert.assertThat(mOutput.toString(), CoreMatchers.containsString("already exists"));
  }

  @Test
  public void copyFromLocalTestWithFullURI() throws IOException, AlluxioException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String alluxioURI = "alluxio://" + mLocalAlluxioCluster.getHostname() + ":"
        + mLocalAlluxioCluster.getMasterRpcPort() + "/destFileURI";
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
  public void copyFromLocalWildcardExistingDir() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    mFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = mFsShell.run("copyFromLocal", testDir + "/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardHier() throws IOException {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    int ret = mFsShell.run("copyFromLocal", testDir + "/*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/bar/foobar3")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalWildcardNotDir() throws IOException, AlluxioException {
    String localTestDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    String alluxioTestDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("copyFromLocal", localTestDir + "/*/foo*", alluxioTestDir + "/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyFromLocalWildcard() throws IOException {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(mLocalAlluxioCluster);
    int ret = mFsShell.run("copyFromLocal", testDir + "/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
    Assert.assertFalse(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalRelativePath() throws Exception {
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put("user.dir", mTestFolder.getRoot().getAbsolutePath());
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      File localDir = mTestFolder.newFolder("testDir");
      generateRelativeFileContent(localDir.getPath() + "/testFile",
              BufferUtils.getIncreasingByteArray(10));
      int ret = mFsShell.run("copyFromLocal", "testDir/testFile", "/testFile");
      Assert.assertEquals(0, ret);
      Assert.assertTrue(fileExists(new AlluxioURI(("/testFile"))));
    }
  }
}
