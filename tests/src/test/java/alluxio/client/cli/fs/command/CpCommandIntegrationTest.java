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

import static org.hamcrest.CoreMatchers.containsString;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.Mode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for cp command.
 */
@LocalAlluxioClusterResource.ServerConfig(
    // do not cache worker info on master or clients, since this test restarts workers
    confParams = {
        PropertyKey.Name.MASTER_WORKER_INFO_CACHE_REFRESH_TIME, "1ms",
        PropertyKey.Name.USER_WORKER_LIST_REFRESH_INTERVAL, "1ms"})
public final class CpCommandIntegrationTest extends AbstractFileSystemShellTest {

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap
      .of(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName()),
      ServerConfiguration.global());

  /**
   * Tests copying a file to a new location.
   */
  @Test
  public void copyFileNew() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", testDir + "/foobar4", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy")));

    Assert.assertTrue(equals(new AlluxioURI("/copy"), new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests copying a file to an existing directory.
   */
  @Test
  public void copyFileExisting() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", testDir + "/foobar4", testDir + "/bar");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + "/bar/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI(testDir + "/bar/foobar4"), new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests recursively copying a directory to a new location.
   */
  @Test
  public void copyDirNew() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", "-R", testDir, "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/bar")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/bar/foobar3")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foo")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foo/foobar1")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foo/foobar2")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foobar4")));

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
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", "-R", testDir, testDir);
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir)));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir + "/bar")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir + "/bar/foobar3")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir + "/foo")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir + "/foo/foobar1")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir + "/foo/foobar2")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + testDir + "/foobar4")));

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
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", testDir + "/*/foo*", "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foobar1")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foobar2")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foobar3")));
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar1"), new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar2"), new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar3"), new AlluxioURI(testDir + "/bar/foobar3")));
  }

  /**
   * Tests copying a list of files with special characters in folder name
   * specified through a wildcard expression.
   */
  @Test
  public void copyWildcardWithSpecialCharacters() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    char[] specialChars = new char[]{'.', '+', '^', '$'};
    for (char specialChar : specialChars) {
      copyWildcardWithSpecialChar(testDir, specialChar);
    }
  }

  private void copyWildcardWithSpecialChar(String testDir, char specialChar) throws Exception {
    String specialFolderName = String.format("%s/folder%sname", testDir, specialChar);
    sFsShell.run("mkdir", specialFolderName);
    sFsShell.run("mkdir", "/result");
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(specialFolderName)));
    sFsShell.run("cp", testDir + "/foobar4", specialFolderName + "/foobar4");
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(specialFolderName + "/foobar4")));
    sFsShell.run("cp", specialFolderName + "/*", "/result/");
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/result/foobar4")));
    sFsShell.run("rm", "/result/foobar4");
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI("/result/foobar4")));
  }

  /**
   * Tests copying a file with attributes preserved.
   */
  @Test
  public void copyFileWithPreservedAttributes() throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
    // avoid chown on UFS since test might not be run with root
    conf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    try (FileSystemShell fsShell = new FileSystemShell(conf)) {
      String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
      AlluxioURI srcFile = new AlluxioURI(testDir + "/foobar4");
      String owner = TEST_USER_1.getUser();
      String group = "staff";
      short mode = 0422;
      List<AclEntry> entries = new ArrayList<>();
      entries.add(new AclEntry.Builder().setType(AclEntryType.NAMED_USER)
          .setSubject(TEST_USER_2.getUser()).addAction(AclAction.READ).addAction(AclAction.WRITE)
          .addAction(AclAction.EXECUTE).build());
      entries.add(new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP).setSubject(group)
          .addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
      sFileSystem.setAttribute(srcFile,
          SetAttributePOptions.newBuilder()
              .setOwner(owner).setGroup(group)
              .setMode(new Mode(mode).toProto())
              .setPinned(true)
              .setReplicationMin(2)
              .setReplicationMax(4)
              .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(12345))
              .build());
      sFileSystem.setAcl(srcFile, SetAclAction.MODIFY, entries);
      int ret = fsShell.run("cp", "-p", testDir + "/foobar4", testDir + "/bar");
      AlluxioURI dstFile = new AlluxioURI(testDir + "/bar/foobar4");
      Assert.assertEquals(0, ret);
      Assert.assertTrue(sFileSystem.exists(dstFile));
      verifyPreservedAttributes(srcFile, dstFile);
    }
  }

  /**
   * Tests copying a folder with attributes preserved.
   */
  @Test
  public void copyDirectoryWithPreservedAttributes() throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
    conf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    try (FileSystemShell fsShell = new FileSystemShell(conf)) {
      String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
      String newDir = "/copy";
      String subDir = "/foo";
      String file = "/foobar4";
      String owner = TEST_USER_1.getUser();
      String group = "staff";
      short mode = 0422;
      List<AclEntry> entries = new ArrayList<>();
      entries.add(new AclEntry.Builder().setType(AclEntryType.NAMED_USER)
          .setSubject(TEST_USER_2.getUser()).addAction(AclAction.READ).addAction(AclAction.WRITE)
          .addAction(AclAction.EXECUTE).build());
      entries.add(new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP).setSubject(group)
          .addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
      AlluxioURI srcDir = new AlluxioURI(testDir);
      sFileSystem.setAttribute(srcDir,
          SetAttributePOptions.newBuilder().setRecursive(true)
              .setOwner(owner).setGroup(group)
              .setMode(new Mode(mode).toProto())
              .setPinned(true)
              .setReplicationMin(2)
              .setReplicationMax(4)
              .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(12345))
              .build());
      sFileSystem.setAcl(srcDir, SetAclAction.MODIFY, entries,
          SetAclPOptions.newBuilder().setRecursive(true).build());
      int ret = fsShell.run("cp", "-R",  "-p", testDir, newDir);
      AlluxioURI dstDir = new AlluxioURI(newDir);
      Assert.assertEquals(0, ret);
      Assert.assertTrue(sFileSystem.exists(dstDir));
      verifyPreservedAttributes(srcDir, dstDir);
      verifyPreservedAttributes(srcDir.join(subDir), dstDir.join(subDir));
      verifyPreservedAttributes(srcDir.join(file), dstDir.join(file));
    }
  }

  private void verifyPreservedAttributes(AlluxioURI src, AlluxioURI dst)
      throws IOException, AlluxioException {
    URIStatus srcStatus = sFileSystem.getStatus(src);
    URIStatus dstStatus = sFileSystem.getStatus(dst);
    Assert.assertEquals(srcStatus.getOwner(), dstStatus.getOwner());
    Assert.assertEquals(srcStatus.getGroup(), dstStatus.getGroup());
    Assert.assertEquals(srcStatus.getMode(), dstStatus.getMode());
    Assert.assertEquals(srcStatus.getAcl(), dstStatus.getAcl());
    Assert.assertNotEquals(srcStatus.getTtl(), dstStatus.getTtl());
    if (!srcStatus.isFolder()) {
      Assert.assertNotEquals(srcStatus.getReplicationMin(), dstStatus.getReplicationMin());
      Assert.assertNotEquals(srcStatus.getReplicationMax(), dstStatus.getReplicationMax());
    }
    Assert.assertNotEquals(srcStatus.isPinned(), dstStatus.isPinned());
  }

  /**
   * Tests invalid input arguments.
   */
  @Test
  public void copyInvalidArgs() throws Exception {
    FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret;
    // cannot copy a directory without -R
    ret = sFsShell.run("cp", "/testDir", "/copy");
    Assert.assertEquals(-1, ret);
    // cannot copy a directory onto an existing file
    ret = sFsShell.run("cp", "-R", "/testDir", "/testDir/foobar4");
    Assert.assertEquals(-1, ret);
    // cannot copy list of file onto a existing file
    ret = sFsShell.run("cp", "-R", "/testDir/*", "/testDir/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyAfterWorkersNotAvailable() throws Exception {
    File testFile = new File(sLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    testFile.delete();
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(100);
    fos.write(toWrite);
    fos.close();

    sFsShell.run("copyFromLocal", testFile.getPath(), "/");
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/testFile")));
    sLocalAlluxioCluster.stopWorkers();
    sFsShell.run("cp", "/testFile", "/testFile2");
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI("/testFile2")));
  }

  @Test
  public void copyAfterWorkersNotAvailableMustCache() throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
    conf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    try (FileSystemShell fsShell = new FileSystemShell(conf)) {
      File testFile = new File(sLocalAlluxioCluster.getAlluxioHome() + "/testFile");
      testFile.createNewFile();
      FileOutputStream fos = new FileOutputStream(testFile);
      byte[] toWrite = BufferUtils.getIncreasingByteArray(100);
      fos.write(toWrite);
      fos.close();

      fsShell.run("copyFromLocal", testFile.getPath(), "/");
      Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/testFile")));
      sLocalAlluxioCluster.stopWorkers();
      fsShell.run("cp", "/testFile", "/testFile2");
      Assert.assertFalse(sFileSystem.exists(new AlluxioURI("/testFile2")));
    }
  }

  private boolean equals(AlluxioURI file1, AlluxioURI file2) throws Exception {
    try (Closer closer = Closer.create()) {
      OpenFilePOptions openFileOptions =
          OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
      FileInStream is1 = closer.register(sFileSystem.openFile(file1, openFileOptions));
      FileInStream is2 = closer.register(sFileSystem.openFile(file2, openFileOptions));
      return IOUtils.contentEquals(is1, is2);
    }
  }

  @Test
  public void copyFromLocalFileToDstPath() throws Exception {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    sFsShell.run("mkdir", "/dstDir");
    sFsShell.run("cp", "file://" + localFile.getPath(), "/dstDir");

    AlluxioURI uri = new AlluxioURI("/dstDir/testFile");
    URIStatus status = sFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    byte[] read = readContent(uri, data.length);
    Assert.assertEquals(new String(read), dataString);
  }

  @Test
  public void copyFromLocalDir() throws Exception {
    // Copy a directory from local to Alluxio filesystem, which the destination uri was not created
    // before.
    File srcOuterDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/outerDir");
    File srcInnerDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/outerDir/innerDir");
    File emptyDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/outerDir/emptyDir");
    srcOuterDir.mkdir();
    srcInnerDir.mkdir();
    emptyDir.mkdir();
    generateFileContent("/outerDir/srcFile1", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/outerDir/innerDir/srcFile2", BufferUtils.getIncreasingByteArray(10));
    int ret = sFsShell.run("cp", "file://" + srcOuterDir.getPath() + "/", "/dstDir");
    Assert.assertEquals(0, ret);
    AlluxioURI dstURI1 = new AlluxioURI("/dstDir/srcFile1");
    AlluxioURI dstURI2 = new AlluxioURI("/dstDir/innerDir/srcFile2");
    AlluxioURI dstURI3 = new AlluxioURI("/dstDir/emptyDir");
    Assert.assertNotNull(sFileSystem.getStatus(dstURI1));
    Assert.assertNotNull(sFileSystem.getStatus(dstURI2));
    Assert.assertNotNull(sFileSystem.getStatus(dstURI3));
  }

  @Test
  public void copyFromLocalDirToExistingFile() throws Exception {
    // Copy a directory from local to a file which exists in Alluxio filesystem. This case should
    // fail.
    File localDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/localDir");
    File innerDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/localDir/innerDir");
    localDir.mkdir();
    innerDir.mkdir();
    generateFileContent("/localDir/srcFile", BufferUtils.getIncreasingByteArray(10));
    sFileSystem.createFile(new AlluxioURI("/dstFile")).close();
    int ret = sFsShell.run("cp", "file://" + localDir.getPath(), "/dstFile");
    Assert.assertEquals(-1, ret);
    Assert.assertFalse(sFileSystem.getStatus(new AlluxioURI("/dstFile")).isFolder());
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI("/dstFile/innerDir")));
  }

  @Test
  public void copyFromLocalDirToExistingDir() throws Exception {
    // Copy a directory from local to Alluxio filesystem, which the destination uri has been
    // created before.
    File srcOuterDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/outerDir");
    File srcInnerDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/outerDir/innerDir");
    File emptyDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/outerDir/emptyDir");
    srcOuterDir.mkdir();
    srcInnerDir.mkdir();
    emptyDir.mkdir();
    generateFileContent("/outerDir/srcFile1", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/outerDir/innerDir/srcFile2", BufferUtils.getIncreasingByteArray(10));
    // Copying a directory to a destination directory which exists and doesn't contain the copied
    // directory.
    sFileSystem.createDirectory(new AlluxioURI("/dstDir"));
    int ret = sFsShell.run("cp", "file://" + srcOuterDir.getPath(), "/dstDir");
    Assert.assertEquals(0, ret);
    AlluxioURI dstURI1 = new AlluxioURI("/dstDir/srcFile1");
    AlluxioURI dstURI2 = new AlluxioURI("/dstDir/innerDir/srcFile2");
    AlluxioURI dstURI3 = new AlluxioURI("/dstDir/emptyDir");
    Assert.assertNotNull(sFileSystem.getStatus(dstURI1));
    Assert.assertNotNull(sFileSystem.getStatus(dstURI2));
    Assert.assertNotNull(sFileSystem.getStatus(dstURI3));

    // Copying a directory to a destination directory which exists and does contain the copied
    // directory.
    sFileSystem.createDirectory(new AlluxioURI("/dstDir1"));
    sFileSystem.createDirectory(new AlluxioURI("/dstDir1/innerDir"));
    int ret1 = sFsShell.run("cp", "file://" + srcOuterDir.getPath(), "/dstDir1");
    Assert.assertEquals(-1, ret1);
    dstURI1 = new AlluxioURI("/dstDir1/srcFile1");
    dstURI2 = new AlluxioURI("/dstDir1/innerDir/srcFile2");
    dstURI3 = new AlluxioURI("/dstDir1/emptyDir");
    Assert.assertNotNull(sFileSystem.getStatus(dstURI1));
    // The directory already exists. But the sub directory shouldn't be copied.
    Assert.assertFalse(sFileSystem.exists(dstURI2));
    Assert.assertNotNull(sFileSystem.getStatus(dstURI3));
  }

  @Test
  public void copyFromLocalLarge() throws Exception {
    File testFile = new File(sLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    testFile.delete();
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    String[] cmd = new String[]{"cp", "file://" +  testFile.getAbsolutePath(), "/testFile"};
    sFsShell.run(cmd);
    Assert.assertEquals(getCommandOutput(cmd), mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = sFileSystem.getStatus(uri);
    Assert.assertNotNull(status);
    Assert.assertEquals(SIZE_BYTES, status.getLength());

    try (FileInStream tfis = sFileSystem.openFile(uri,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
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
    sFsShell.run(cmd1);
    Assert.assertEquals(getCommandOutput(cmd1), mOutput.toString());
    mOutput.reset();
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(alluxioFilePath, LEN1)));

    // Write the second file to the same location, which should cause an exception
    String[] cmd2 = {"cp", "file://" +  testFile2.getPath(), alluxioFilePath.getPath()};
    Assert.assertEquals(-1, sFsShell.run(cmd2));
    Assert.assertThat(mOutput.toString(), containsString(
        "Not allowed to create file because path already exists: " + alluxioFilePath.getPath()));
    // Make sure the original file is intact
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteArray(LEN1, readContent(alluxioFilePath, LEN1)));
  }

  @Test
  public void copyFromLocal() throws Exception {
    File testDir = new File(sLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(sLocalAlluxioCluster.getAlluxioHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile =
        generateFileContent("/testDir/testFile", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        BufferUtils.getIncreasingByteArray(10, 20));
    String[] cmd = new String[]{"cp", "file://" + testFile.getParent(), "/testDir"};
    sFsShell.run(cmd);
    Assert.assertThat(mOutput.toString(), containsString(getCommandOutput(cmd)));
    AlluxioURI uri1 = new AlluxioURI("/testDir/testFile");
    AlluxioURI uri2 = new AlluxioURI("/testDir/testDirInner/testFile2");
    URIStatus status1 = sFileSystem.getStatus(uri1);
    URIStatus status2 = sFileSystem.getStatus(uri2);
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
    String alluxioURI = "alluxio://" + sLocalAlluxioCluster.getHostname() + ":"
        + sLocalAlluxioCluster.getMasterRpcPort() + "/destFileURI";
    String[] cmd = new String[]{"cp", "file://" + testFile.getPath(), alluxioURI};
    // when
    sFsShell.run(cmd);
    String cmdOut = getCommandOutput(cmd);
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    AlluxioURI uri = new AlluxioURI("/destFileURI");
    URIStatus status = sFileSystem.getStatus(uri);
    Assert.assertEquals(10L, status.getLength());
    byte[] read = readContent(uri, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyFromLocalWildcardExistingDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(sLocalAlluxioCluster);
    sFileSystem.createDirectory(new AlluxioURI("/testDir"));
    int ret = sFsShell.run("cp", "file://" +  testDir + "/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardHier() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(sLocalAlluxioCluster);
    int ret = sFsShell.run("cp", "file://" +  testDir + "/*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foo/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/bar/foobar3")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalWildcardNotDir() throws Exception {
    String localTestDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    String alluxioTestDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", "file://" +  localTestDir + "/*/foo*", alluxioTestDir
                           + "/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyFromLocalWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetLocalFileHierarchy(sLocalAlluxioCluster);
    int ret = sFsShell.run("cp", "file://" +  testDir + "/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar1")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar2")));
    Assert.assertTrue(fileExists(new AlluxioURI("/testDir/foobar3")));
    Assert.assertFalse(fileExists(new AlluxioURI("/testDir/foobar4")));
  }

  @Test
  public void copyToLocalDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", testDir,
            "file://" + sLocalAlluxioCluster.getAlluxioHome() + "/testDir");
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
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    new File(sLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();

    int ret = sFsShell.run("cp", testDir + "/*/foo*",
        "file://" + sLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalWildcardHier() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell
        .run("cp", testDir + "/*", "file://" + sLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocalWildcardNotDir() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    new File(sLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();
    File testFile = new File(sLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile");
    testFile.delete();
    testFile.createNewFile();

    int ret = sFsShell.run("cp", testDir + "/*/foo*",
        "file://" + sLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyToLocalWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", testDir + "/*/foo*",
        "file://" + sLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Override
  protected void copyToLocalWithBytes(int bytes) throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE,
        bytes);
    String[] cmd = new String[] {"cp", "/testFile",
        "file://" + sLocalAlluxioCluster.getAlluxioHome() + "/testFile"};
    sFsShell.run(cmd);
    Assert.assertEquals(getCommandOutput(cmd), mOutput.toString());
    fileReadTest("/testFile", 10);
  }
}
