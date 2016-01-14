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

package tachyon.shell;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterContext;
import tachyon.security.LoginUser;
import tachyon.security.authentication.AuthType;
import tachyon.security.group.provider.IdentityUserGroupsMapping;
import tachyon.shell.command.CommandUtils;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Unit tests on {@link TfsShell}.
 */
public class TfsShellTest {
  private static final int SIZE_BYTES = Constants.MB * 10;
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(SIZE_BYTES, 1000, Constants.MB,
          Constants.MASTER_TTLCHECKER_INTERVAL_MS, String.valueOf(Integer.MAX_VALUE),
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private TfsShell mFsShell = null;
  private ByteArrayOutputStream mOutput = null;
  private PrintStream mNewOutput = null;
  private PrintStream mOldOutput = null;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mFsShell.close();
    System.setOut(mOldOutput);
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = mLocalTachyonClusterResource.get();
    mTfs = mLocalTachyonCluster.getClient();
    mFsShell = new TfsShell(new TachyonConf());
    mOutput = new ByteArrayOutputStream();
    mNewOutput = new PrintStream(mOutput);
    mOldOutput = System.out;
    System.setOut(mNewOutput);
  }

  @Test
  public void catDirectoryTest() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    int ret = mFsShell.run("cat", "/testDir");
    Assert.assertEquals(-1, ret);
    String expected = getCommandOutput(command);
    expected += "Path /testDir must be a file\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void catNotExistTest() throws IOException {
    int ret = mFsShell.run("cat", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void catTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 10);
    mFsShell.run("cat", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
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
    TachyonFile tFile = mTfs.open(new TachyonURI("/testFile"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(SIZE_BYTES, fileInfo.length);

    InStreamOptions options = new InStreamOptions.Builder(new TachyonConf())
        .setTachyonStorageType(TachyonStorageType.NO_STORE).build();
    FileInStream tfis = mTfs.getInStream(tFile, options);
    byte[] read = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void loadFileTest() throws IOException, TachyonException {
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, "/testFile",
        TachyonStorageType.NO_STORE, UnderStorageType.SYNC_PERSIST, 10);
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertFalse(fileInfo.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("load", "/testFile");
    fileInfo = mTfs.getInfo(file);
    Assert.assertTrue(fileInfo.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadDirTest() throws IOException, TachyonException {
    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA",
        TachyonStorageType.NO_STORE, UnderStorageType.SYNC_PERSIST, 10);
    TachyonFile fileB = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 10);
    FileInfo fileInfoA = mTfs.getInfo(fileA);
    FileInfo fileInfoB = mTfs.getInfo(fileB);
    Assert.assertFalse(fileInfoA.getInMemoryPercentage() == 100);
    Assert.assertTrue(fileInfoB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("load", "/testRoot");
    fileInfoA = mTfs.getInfo(fileA);
    fileInfoB = mTfs.getInfo(fileB);
    Assert.assertTrue(fileInfoA.getInMemoryPercentage() == 100);
    Assert.assertTrue(fileInfoB.getInMemoryPercentage() == 100);
  }

  @Test
  public void lsThenloadMetadataTest() throws IOException, TachyonException {
    TachyonConf conf = mLocalTachyonCluster.getMasterTachyonConf();
    String ufsRoot = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(ufsRoot, "dir1"), conf);
    // First run ls to create the data
    mFsShell.run("ls", "/dir1");
    Assert.assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/dir1"))).isIsPersisted());
    // Load metadata
    mFsShell.run("loadMetadata", "/dir1");
    Assert.assertEquals(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage("/dir1") + "\n",
        mOutput.toString());
  }

  @Test
  public void loadMetadataErrorMessageTest() {
    int ret = mFsShell.run("loadMetadata", "/nonexistent");
    Assert.assertEquals(-1, ret);
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nonexistent") + "\n",
        mOutput.toString());
  }

  @Test
  public void createCacheInsertInUfsThenloadMetadataTest() throws IOException, TachyonException {
    // Construct a situation where the directory exists in the inode tree and the UFS, but is not
    // marked as persisted.
    TachyonFSTestUtils.createByteFile(mTfs, "/testDir/testFileA", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 10);
    Assert.assertFalse(mTfs.getInfo(mTfs.open(new TachyonURI("/testDir"))).isIsPersisted());
    TachyonConf conf = mLocalTachyonCluster.getMasterTachyonConf();
    String ufsRoot = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(ufsRoot, "testDir"), conf);
    Assert.assertFalse(mTfs.getInfo(mTfs.open(new TachyonURI("/testDir"))).isIsPersisted());
    // Load metadata, which should mark the testDir as persisted
    mFsShell.run("loadMetadata", "/testDir");
    Assert.assertEquals("", mOutput.toString());
    Assert.assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testDir"))).isIsPersisted());
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
    TachyonFile file1 = mTfs.open(new TachyonURI("/testDir/testFile"));
    TachyonFile file2 = mTfs.open(new TachyonURI("/testDir/testDirInner/testFile2"));
    FileInfo fileInfo1 = mTfs.getInfo(file1);
    FileInfo fileInfo2 = mTfs.getInfo(file2);
    Assert.assertNotNull(fileInfo1);
    Assert.assertNotNull(fileInfo2);
    Assert.assertEquals(10, fileInfo1.length);
    Assert.assertEquals(20, fileInfo2.length);
    byte[] read = readContent(file1, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
    read = readContent(file2, 20);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, 20, read));
  }

  @Test
  public void copyFromLocalTestWithFullURI() throws IOException, TachyonException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String tachyonURI = "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.run("copyFromLocal", testFile.getPath(), tachyonURI);
    String cmdOut =
        getCommandOutput(new String[]{"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    TachyonFile file = mTfs.open(new TachyonURI("/destFileURI"));
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertEquals(10L, fileInfo.length);
    byte[] read = readContent(file, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
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

    TachyonFile file = mTfs.open(new TachyonURI("/dstDir/testFile"));
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertNotNull(fileInfo);
    byte[] read = readContent(file, data.length);
    Assert.assertEquals(new String(read), dataString);
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
    TachyonFile tFile = mTfs.open(tachyonFilePath);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(LEN1, readContent(tFile, LEN1)));

    // Write the second file to the same location, which should cause an exception
    String[] cmd2 = {"copyFromLocal", testFile2.getPath(), tachyonFilePath.getPath()};
    Assert.assertEquals(-1, mFsShell.run(cmd2));
    Assert.assertEquals(tachyonFilePath.getPath() + " already exists\n", mOutput.toString());
    // Make sure the original file is intact
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(LEN1, readContent(tFile, LEN1)));
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

    // Make sure the tachyon file wasn't created anyways
    Assert.assertNull(mTfs.openIfExists(tachyonFilePath));
  }

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
    Assert.assertNull(mTfs.openIfExists(tachyonDirPath));
    mOutput.reset();

    // If we put a copyable file in the directory, we should be able to copy just that file
    generateFileContent("/localDir/testFile2", BufferUtils.getIncreasingByteArray(20));
    Assert.assertEquals(-1, mFsShell.run(cmd));
    Assert.assertEquals(testFile.getPath() + " (Permission denied)\n", mOutput.toString());
    Assert.assertNotNull(mTfs.openIfExists(tachyonDirPath));
    Assert.assertNotNull(mTfs.openIfExists(new TachyonURI("/testDir/testFile2")));
    Assert.assertNull(mTfs.openIfExists(new TachyonURI("/testDir/testFile")));
  }

  @Test
  public void copyToLocalLargeTest() throws IOException {
    copyToLocalWithBytes(SIZE_BYTES);
  }

  @Test
  public void copyToLocalTest() throws IOException {
    copyToLocalWithBytes(10);
  }

  private void copyToLocalWithBytes(int bytes) throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, bytes);
    mFsShell.run("copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile");
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    fileReadTest("/testFile", 10);
  }

  @Test
  public void copyToLocalDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    int ret = mFsShell.run("copyToLocal", "/testWildCards/",
        mLocalTachyonCluster.getTachyonHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  private void fileReadTest(String fileName, int size) throws IOException {
    File testFile = new File(PathUtils.concatPath(mLocalTachyonCluster.getTachyonHome(), fileName));
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[size];
    fis.read(read);
    fis.close();
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, read));
  }

  @Test
  public void countNotExistTest() throws IOException {
    int ret = mFsShell.run("count", "/NotExistFile");
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void countTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 30);
    mFsShell.run("count", "/testRoot");
    String expected = "";
    String format = "%-25s%-25s%-15s\n";
    expected += String.format(format, "File Count", "Folder Count", "Total Bytes");
    expected += String.format(format, 3, 2, 60);
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void fileinfoNotExistTest() throws IOException {
    int ret = mFsShell.run("fileinfo", "/NotExistFile");
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  /*
   * @Test public void fileInfoTest() throws IOException { TachyonFile file =
   * TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.STORE,
   * UnderStorageType.NO_PERSIST, 10); mFsShell.run(new String[] {"fileinfo", "/testFile"});
   * FileInfo fileInfo = mTfs.getInfo(file); Assert.assertNotNull(fileInfo); List<FileBlockInfo>
   * blocks = mTfs.getFileBlocks(fileId); String[] commandParameters = new String[3 +
   * blocks.size()]; commandParameters[0] = "fileinfo"; commandParameters[1] = "/testFile";
   * commandParameters[2] = String.valueOf(file.getFileId()); Iterator<FileBlockInfo> iter =
   * blocks.iterator(); int i = 3; while (iter.hasNext()) { commandParameters[i ++] =
   * iter.next().toString(); } Assert.assertEquals(getCommandOutput(commandParameters),
   * mOutput.toString()); }
   */
  private File generateFileContent(String path, byte[] toWrite)
      throws IOException, FileNotFoundException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + path);
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(toWrite);
    fos.close();
    return testFile;
  }

  private String getCommandOutput(String[] command) {
    String cmd = command[0];
    if (command.length == 2) {
      if (cmd.equals("ls")) {
        // Not sure how to handle this one.
        return null;
      } else if (cmd.equals("mkdir")) {
        return "Successfully created directory " + command[1] + "\n";
      } else if (cmd.equals("rm") || cmd.equals("rmr")) {
        return command[1] + " has been removed" + "\n";
      } else if (cmd.equals("touch")) {
        return command[1] + " has been created" + "\n";
      }
    } else if (command.length == 3) {
      if (cmd.equals("mv")) {
        return "Renamed " + command[1] + " to " + command[2] + "\n";
      } else if (cmd.equals("copyFromLocal")) {
        return "Copied " + command[1] + " to " + command[2] + "\n";
      } else if (cmd.equals("copyToLocal")) {
        return "Copied " + command[1] + " to " + command[2] + "\n";
      }
    } else if (command.length > 3) {
      if (cmd.equals("location")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " is on nodes: \n");
        for (int i = 3; i < command.length; i ++) {
          ret.append(command[i] + "\n");
        }
        return ret.toString();
      } else if (cmd.equals("fileinfo")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " has the following blocks: \n");
        for (int i = 3; i < command.length; i ++) {
          ret.append(command[i] + "\n");
        }
        return ret.toString();
      }
    }
    return null;
  }

  @Test
  public void locationNotExistTest() throws IOException {
    int ret = mFsShell.run("location", "/NotExistFile");
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  /*
   * @Test public void locationTest() throws IOException { TachyonFile file =
   * TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.STORE,
   * UnderStorageType.NO_PERSIST, 10); mFsShell.run(new String[] {"location", "/testFile"});
   *
   * FileInfo fileInfo = mTfs.getInfo(file); Assert.assertNotNull(fileInfo); List<String>
   * locationsList = tFile.getLocationHosts(); String[] commandParameters = new String[3 +
   * locationsList.size()]; commandParameters[0] = "location"; commandParameters[1] = "/testFile";
   * commandParameters[2] = String.valueOf(file.getFileId()); Iterator<String> iter =
   * locationsList.iterator(); int i = 3; while (iter.hasNext()) { commandParameters[i ++] =
   * iter.next(); } Assert.assertEquals(getCommandOutput(commandParameters), mOutput.toString()); }
   */

  @Test
  public void lsrTest() throws IOException, TachyonException {
    // clear the loginUser
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    FileInfo[] files = new FileInfo[4];
    String testUser = "test_user_lsr";
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, testUser);

    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 10);
    files[0] = mTfs.getInfo(fileA);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 20);
    files[1] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir")));
    files[2] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir/testFileB")));
    TachyonFile fileC = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC",
        TachyonStorageType.NO_STORE, UnderStorageType.SYNC_PERSIST, 30);
    files[3] = mTfs.getInfo(fileC);
    mFsShell.run("lsr", "/testRoot");
    String expected = "";
    expected +=
        getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10, "In Memory",
            testUser, testUser);
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 0, "", testUser,
            testUser);
    expected +=
        getLsResultStr("/testRoot/testDir/testFileB", files[2].getCreationTimeMs(), 20,
            "In Memory", testUser, testUser);
    expected +=
        getLsResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30, "Not In Memory",
            testUser, testUser);
    Assert.assertEquals(expected, mOutput.toString());
    // clear testing username
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.reset();
  }

  @Test
  public void lsTest() throws IOException, TachyonException {
    // clear the loginUser
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    FileInfo[] files = new FileInfo[4];
    String testUser = "test_user_ls";
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, testUser);

    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 10);
    files[0] = mTfs.getInfo(fileA);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 20);
    files[1] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir")));
    TachyonFile fileC = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC",
        TachyonStorageType.NO_STORE, UnderStorageType.SYNC_PERSIST, 30);
    files[2] = mTfs.getInfo(fileC);
    mFsShell.run("ls", "/testRoot");
    String expected = "";
    expected +=
        getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10, "In Memory",
            testUser, testUser);
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 0, "", testUser,
            testUser);
    expected +=
        getLsResultStr("/testRoot/testFileC", files[2].getCreationTimeMs(), 30, "Not In Memory",
            testUser, testUser);
    Assert.assertEquals(expected, mOutput.toString());
    // clear testing username
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.reset();
  }

  @Test
  public void mkdirComplexPathTest() throws IOException, TachyonException {
    mFsShell.run("mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File");
    TachyonFile tFile = mTfs.open(new TachyonURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(new String[]{"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,"
        + ".?/File"}), mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }

  @Test
  public void mkdirExistingTest() throws IOException {
    Assert.assertEquals(0, mFsShell.run("mkdir", "/testFile1"));
    Assert.assertEquals(-1, mFsShell.run("mkdir", "/testFile1"));
  }

  @Test
  public void mkdirInvalidPathTest() throws IOException {
    Assert.assertEquals(-1, mFsShell.run("mkdir", "/test File Invalid Path"));
  }

  @Test
  public void mkdirMultiPathTest() throws IOException, TachyonException {
    String path1 = "/testDir1";
    String path2 = "/testDir2";
    String path3 = "/testDir2/testDir2.1";
    Assert.assertEquals(0, mFsShell.run("mkdir", path1, path2, path3));

    TachyonFile tFile = mTfs.open(new TachyonURI(path1));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.isIsFolder());

    tFile = mTfs.open(new TachyonURI(path2));
    fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.isIsFolder());

    tFile = mTfs.open(new TachyonURI(path3));
    fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.isIsFolder());

  }

  @Test
  public void mkdirShortPathTest() throws IOException, TachyonException {
    mFsShell.run("mkdir", "/root/testFile1");
    TachyonFile tFile = mTfs.open(new TachyonURI("/root/testFile1"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(new String[]{"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }

  @Test
  public void mkdirTest() throws IOException, TachyonException {
    String qualifiedPath = "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/root/testFile1";
    mFsShell.run("mkdir", qualifiedPath);
    TachyonFile tFile = mTfs.open(new TachyonURI("/root/testFile1"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert
        .assertEquals(getCommandOutput(new String[] {"mkdir", qualifiedPath}), mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }

  private byte[] readContent(TachyonFile tFile, int length) throws IOException, TachyonException {
    InStreamOptions options = new InStreamOptions.Builder(new TachyonConf())
        .setTachyonStorageType(TachyonStorageType.NO_STORE).build();
    FileInStream tfis = mTfs.getInStream(tFile, options);
    byte[] read = new byte[length];
    tfis.read(read);
    return read;
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/test/File1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    mFsShell.run("mv", "/test", "/test2");
    toCompare.append(getCommandOutput(new String[]{"mv", "/test", "/test2"}));
    Assert.assertTrue(fileExist(new TachyonURI("/test2/File1")));
    Assert.assertFalse(fileExist(new TachyonURI("/test")));
    Assert.assertFalse(fileExist(new TachyonURI("/test/File1")));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void renameTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertTrue(fileExist(new TachyonURI("/testFolder1")));
    mFsShell.run("mv", "/testFolder1", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExist(new TachyonURI("/testFolder")));
    Assert.assertFalse(fileExist(new TachyonURI("/testFolder1")));
  }

  @Test
  public void renameToExistingFileTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.run("mkdir", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    int ret = mFsShell.run("mv", "/testFolder1", "/testFolder");

    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertTrue(output.contains("mv: Failed to rename /testFolder1 to /testFolder"));
  }

  @Test
  public void rmNotExistingFileTest() throws IOException {
    mFsShell.run("rm", "/testFile");
    String expected = ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testFile") + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void rmNotExistingDirTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.run("rm", "/testFolder");
    toCompare.append("rm: cannot remove a directory, please try rmr <path>\n");
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void rmTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder1/testFolder2");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.run("touch", "/testFolder1/testFolder2/testFile2");
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(fileExist(testFolder1));
    Assert.assertTrue(fileExist(testFolder2));
    Assert.assertTrue(fileExist(testFile2));
    mFsShell.run("rm", "/testFolder1/testFolder2/testFile2");
    toCompare.append(getCommandOutput(new String[]{"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExist(testFolder1));
    Assert.assertTrue(fileExist(testFolder2));
    Assert.assertFalse(fileExist(testFile2));
  }

  @Test
  public void rmrTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder1/testFolder2");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.run("touch", "/testFolder1/testFolder2/testFile2");
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(fileExist(testFolder1));
    Assert.assertTrue(fileExist(testFolder2));
    Assert.assertTrue(fileExist(testFile2));
    mFsShell.run("rmr", "/testFolder1/testFolder2/testFile2");
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExist(testFolder1));
    Assert.assertTrue(fileExist(testFolder2));
    Assert.assertFalse(fileExist(testFile2));
    mFsShell.run("rmr", "/testFolder1");
    toCompare.append(getCommandOutput(new String[] {"rmr", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertFalse(fileExist(testFolder1));
    Assert.assertFalse(fileExist(testFolder2));
    Assert.assertFalse(fileExist(testFile2));
  }

  @Test
  public void tailEmptyFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/emptyFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 0);
    int ret = mFsShell.run("tail", "/emptyFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void tailLargeFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 2048);
    mFsShell.run("tail", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(1024, 1024);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailNotExistTest() throws IOException {
    int ret = mFsShell.run("tail", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void tailSmallFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 10);
    mFsShell.run("tail", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void touchTest() throws IOException, TachyonException {
    String[] argv = new String[] {"touch", "/testFile"};
    mFsShell.run(argv);
    TachyonFile tFile = mTfs.open(new TachyonURI("/testFile"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(fileInfo.isFolder);
  }

  @Test
  public void touchTestWithFullURI() throws IOException, TachyonException {
    String tachyonURI = "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    String[] argv = new String[] {"touch", tachyonURI};
    mFsShell.run(argv);
    // then
    TachyonFile tFile = mTfs.open(new TachyonURI("/destFileURI"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(fileInfo.isFolder);
  }

  @Test
  public void freeTest() throws IOException, TachyonException {
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, "/testFile",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 10);
    mFsShell.run("free", "/testFile");
    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    CommonUtils.sleepMs(tachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    Assert.assertFalse(mTfs.getInfo(file).getInMemoryPercentage() == 100);
  }

  @Test
  public void duTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testDir/testFileC",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 30);

    String expected = "";
    // du a non-existing file
    mFsShell.run("du", "/testRoot/noneExisting");
    expected += ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testRoot/noneExisting") + "\n";
    // du a file
    mFsShell.run("du", "/testRoot/testFileA");
    expected += "/testRoot/testFileA is 10 bytes\n";
    // du a folder
    mFsShell.run("du", "/testRoot/testDir");
    expected += "/testRoot/testDir is 50 bytes\n";
    Assert.assertEquals(expected.toString(), mOutput.toString());
  }

  @Test
  public void catWildcardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    // the expect contents (remember that the order is based on path)
    byte[] exp1 = BufferUtils.getIncreasingByteArray(30); // testWildCards/bar/foobar3
    byte[] exp2 = BufferUtils.getIncreasingByteArray(10); // testWildCards/foo/foobar1
    byte[] exp3 = BufferUtils.getIncreasingByteArray(20); // testWildCards/foo/foobar2
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = mFsShell.run("cat", "/testWildCards/*/foo*");
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }

  @Test
  public void freeWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();

    int ret = mFsShell.run("free", "/testWild*/foo/*");
    CommonUtils.sleepMs(null,
        tachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar1"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar2"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/foobar4"));

    ret = mFsShell.run("free", "/testWild*/*/");
    CommonUtils.sleepMs(null, tachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)
        * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foobar4"));
  }

  private boolean isInMemoryTest(String path) throws IOException, TachyonException {
    return (mTfs.getInfo(mTfs.open(new TachyonURI(path))).getInMemoryPercentage() == 100);
  }

  @Test
  public void lsWildcardTest() throws IOException, TachyonException {
    // clear the loginUser
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    MasterContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());
    String testUser = "test_user_lsWildcard";
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, testUser);

    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    String expect = "";
    expect += getLsResultStr(new TachyonURI("/testWildCards/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar2"), 20, testUser, testUser);
    mFsShell.run("ls", "/testWildCards/*/foo*");
    Assert.assertEquals(expect, mOutput.toString());

    expect += getLsResultStr(new TachyonURI("/testWildCards/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foo/foobar2"), 20, testUser, testUser);
    expect += getLsResultStr(new TachyonURI("/testWildCards/foobar4"), 40, testUser, testUser);
    mFsShell.run("ls", "/testWildCards/*");
    Assert.assertEquals(expect, mOutput.toString());
    // clear testing username
    System.clearProperty(Constants.SECURITY_LOGIN_USERNAME);
    MasterContext.reset();
  }

  private String getLsResultStr(TachyonURI tUri, int size, String testUser, String testGroup)
      throws IOException, TachyonException {
    return getLsResultStr(tUri.getPath(), mTfs.getInfo(mTfs.open(tUri)).getCreationTimeMs(), size,
        "In Memory", testUser, testGroup);
  }

  private String getLsResultStr(String path, long createTime, int size, String fileType,
      String testUser, String testGroup) throws IOException, TachyonException {
    return String.format(Constants.COMMAND_FORMAT_LS, FormatUtils.getSizeFromBytes(size),
        CommandUtils.convertMsToDate(createTime), fileType, testUser, testGroup, path);
  }

  @Test
  public void rmWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    mFsShell.run("rm", "/testWildCards/foo/foo*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar1")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));

    mFsShell.run("rm", "/testWildCards/*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foobar4")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/foo")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));
  }

  private boolean fileExist(TachyonURI path) {
    try {
      return mTfs.open(path) != null;
    } catch (IOException e) {
      return false;
    } catch (TachyonException e) {
      return false;
    }
  }

  @Test
  public void rmrWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    mFsShell.run("rmr", "/testWildCards/foo/foo*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar1")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/foo")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));

    mFsShell.run("rmr", "/testWildCards/ba*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/bar")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/foobar4")));

    mFsShell.run("rmr", "/testWildCards/*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/bar")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foobar4")));
  }

  @Test
  public void tailWildcardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    // the expect contents (remember that the order is based on the path)
    byte[] exp1 = BufferUtils.getIncreasingByteArray(30); // testWildCards/bar/foobar3
    byte[] exp2 = BufferUtils.getIncreasingByteArray(10); // testWildCards/foo/foobar1
    byte[] exp3 = BufferUtils.getIncreasingByteArray(20); // testWildCards/foo/foobar2
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = mFsShell.run("tail", "/testWildCards/*/foo*");
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }

  @Test
  public void fileinfoWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    mFsShell.run("fileinfo", "/testWildCards/*");
    String res1 = mOutput.toString();
    Assert.assertTrue(res1.contains("/testWildCards/foo"));
    Assert.assertTrue(res1.contains("/testWildCards/bar"));
    Assert.assertTrue(res1.contains("/testWildCards/foobar4"));
    Assert.assertFalse(res1.contains("/testWildCards/foo/foobar1"));
    Assert.assertFalse(res1.contains("/testWildCards/bar/foobar3"));

    mFsShell.run("fileinfo", "/testWildCards/*/foo*");
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    Assert.assertTrue(res2.contains("/testWildCards/foo/foobar1"));
    Assert.assertTrue(res2.contains("/testWildCards/foo/foobar2"));
    Assert.assertTrue(res2.contains("/testWildCards/bar/foobar3"));
    Assert.assertFalse(res2.contains("/testWildCards/foobar4"));
  }

  // private boolean isFileExist(TachyonURI path) {
  // try {
  // mTfs.open(path);
  // return true;
  // } catch (IOException ioe) {
  // return false;
  // }
  // }

  @Test
  public void copyFromLocalWildcardTest() throws IOException {
    TfsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    int ret = mFsShell.run("copyFromLocal",
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar1")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar3")));
    Assert.assertFalse(fileExist(new TachyonURI("/testDir/foobar4")));
  }

  @Test
  public void copyFromLocalWildcardExistingDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    mTfs.mkdir(new TachyonURI("/testDir"));
    int ret = mFsShell.run("copyFromLocal",
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar1")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardNotDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    int ret = mFsShell.run("copyFromLocal",
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testWildCards/foobar4");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyFromLocalWildcardHierTest() throws IOException {
    TfsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    int ret = mFsShell.run("copyFromLocal",
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*", "/testDir");

    mFsShell.run("ls", "/testDir");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foo/foobar1")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foo/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/bar/foobar3")));
    Assert.assertTrue(fileExist(new TachyonURI("/testDir/foobar4")));
  }

  @Test
  public void copyToLocalWildcardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    int ret = mFsShell.run("copyToLocal", "/testWildCards/*/foo*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalWildcardExistingDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    new File(mLocalTachyonCluster.getTachyonHome() + "/testDir").mkdir();

    int ret = mFsShell.run("copyToLocal", "/testWildCards/*/foo*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalWildcardNotDirTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    new File(mLocalTachyonCluster.getTachyonHome() + "/testDir").mkdir();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testDir/testFile").createNewFile();

    int ret = mFsShell.run("copyToLocal", "/testWildCards/*/foo*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyToLocalWildcardHierTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    int ret = mFsShell.run("copyToLocal", "/testWildCards/*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  /**
   * Checks whether the given file is actually persisted by freeing it, then
   * reading it and comparing it against the expected byte array.
   *
   * @param file The file to persist
   * @param size The size of the file
   * @throws TachyonException if an unexpected tachyon exception is thrown
   * @throws IOException if a non-Tachyon exception occurs
   */
  private void checkFilePersisted(TachyonFile file, int size) throws TachyonException, IOException {
    Assert.assertTrue(mTfs.getInfo(file).isIsPersisted());
    mTfs.free(file);
    FileInStream tfis = mTfs.getInStream(file);
    byte[] actual = new byte[size];
    tfis.read(actual);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(size), actual);
  }

  @Test
  public void setTtlNegativeTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile",
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 1);
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage("TTL value must be >= 0");
    mFsShell.run("setTtl", "/testFile", "-1");
  }

  @Test
  public void setTtlTest() throws Exception {
    String filePath = "/testFile";
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, filePath, TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 1);
    Assert.assertEquals(Constants.NO_TTL, mTfs.getInfo(file).getTtl());
    long[] ttls = new long[] { 0L, 1000L };
    for (long ttl : ttls) {
      Assert.assertEquals(0, mFsShell.run("setTtl", filePath, String.valueOf(ttl)));
      Assert.assertEquals(ttl, mTfs.getInfo(file).getTtl());
    }
  }

  @Test
  public void unsetTtlTest() throws Exception {
    String filePath = "/testFile";
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, filePath, TachyonStorageType
        .STORE, UnderStorageType.NO_PERSIST, 1);
    Assert.assertEquals(Constants.NO_TTL, mTfs.getInfo(file).getTtl());

    // unsetTtl on a file originally with no TTL will leave the TTL unchanged.
    Assert.assertEquals(0, mFsShell.run("unsetTtl", filePath));
    Assert.assertEquals(Constants.NO_TTL, mTfs.getInfo(file).getTtl());

    long ttl = 1000L;
    Assert.assertEquals(0, mFsShell.run("setTtl", filePath, String.valueOf(ttl)));
    Assert.assertEquals(ttl, mTfs.getInfo(file).getTtl());
    Assert.assertEquals(0, mFsShell.run("unsetTtl", filePath));
    Assert.assertEquals(Constants.NO_TTL, mTfs.getInfo(file).getTtl());
  }

  @Test
  public void persistTest() throws IOException, TachyonException {
    String testFilePath = "/testPersist/testFile";
    TachyonFile testFile = TachyonFSTestUtils.createByteFile(mTfs, testFilePath,
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 10);
    Assert.assertFalse(mTfs.getInfo(testFile).isIsPersisted());

    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n", mOutput.toString());
    checkFilePersisted(testFile, 10);
  }

  @Test
  public void persistTwiceTest() throws IOException, TachyonException {
    // Persisting an already-persisted file is okay
    String testFilePath = "/testPersist/testFile";
    TachyonFile testFile = TachyonFSTestUtils.createByteFile(mTfs, testFilePath,
        TachyonStorageType.STORE, UnderStorageType.NO_PERSIST, 10);
    Assert.assertFalse(mTfs.getInfo(testFile).isIsPersisted());
    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n" + testFilePath
        + " is already persisted\n", mOutput.toString());
    checkFilePersisted(testFile, 10);
  }

  @Test
  public void persistNonexistentFileTest() throws IOException, TachyonException {
    // Cannot persist a nonexistent file
    String path = "/testPersistNonexistent";
    int ret = mFsShell.run("persist", path);
    Assert.assertEquals(-1, ret);
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path) + "\n",
        mOutput.toString());
  }

  @Test
  public void persistDirectoryTest() throws IOException, TachyonException {
    // Set the default write type to MUST_CACHE, so that directories are not persisted by default
    ClientContext.getConf().set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    Assert.assertFalse(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards"))).isIsPersisted());
    Assert
        .assertFalse(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/foo"))).isIsPersisted());
    Assert
        .assertFalse(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/bar"))).isIsPersisted());
    int ret = mFsShell.run("persist", "/testWildCards");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards"))).isIsPersisted());
    Assert
        .assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/foo"))).isIsPersisted());
    Assert
        .assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/bar"))).isIsPersisted());
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/foo/foobar1")), 10);
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/foo/foobar2")), 20);
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/bar/foobar3")), 30);
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/foobar4")), 40);
    ClientContext.reset();
  }
}
