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
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.CacheType;
import tachyon.client.ClientOptions;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;
import tachyon.util.io.BufferUtils;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  private static final int SIZE_BYTES = Constants.MB * 10;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private TFsShell mFsShell = null;
  private ByteArrayOutputStream mOutput = null;
  private PrintStream mNewOutput = null;
  private PrintStream mOldOutput = null;

  @After
  public final void after() throws Exception {
    mFsShell.close();
    mLocalTachyonCluster.stop();
    System.setOut(mOldOutput);
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(SIZE_BYTES, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mFsShell = new TFsShell(mLocalTachyonCluster.getMasterTachyonConf());
    mOutput = new ByteArrayOutputStream();
    mNewOutput = new PrintStream(mOutput);
    mOldOutput = System.out;
    System.setOut(mNewOutput);
  }

  @Test
  public void catDirectoryTest() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    int ret = mFsShell.run(new String[] {"cat", "/testDir"});
    Assert.assertEquals(-1, ret);
    String expected = getCommandOutput(command);
    expected += "/testDir is not a file.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void catNotExistTest() throws IOException {
    int ret = mFsShell.run(new String[] {"cat", "/testFile"});
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void catTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 10);
    mFsShell.run(new String[] {"cat", "/testFile"});
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void copyFromLocalLargeTest() throws IOException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", testFile.getAbsolutePath(), "/testFile"});
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(), "/testFile"}),
        mOutput.toString());
    TachyonFile tFile = mTfs.open(new TachyonURI("/testFile"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(SIZE_BYTES, fileInfo.length);

    ClientOptions options =
        new ClientOptions.Builder(new TachyonConf()).setCacheType(CacheType.NO_CACHE).build();
    FileInStream tfis = mTfs.getInStream(tFile, options);
    byte[] read = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void loadFileTest() throws IOException {
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.NO_CACHE,
        UnderStorageType.PERSIST, 10);
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertFalse(fileInfo.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run(new String[] {"load", "/testFile"});
    fileInfo = mTfs.getInfo(file);
    Assert.assertTrue(fileInfo.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadDirTest() throws IOException {
    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA",
        CacheType.NO_CACHE, UnderStorageType.PERSIST, 10);
    TachyonFile fileB = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB",
        CacheType.CACHE, UnderStorageType.NO_PERSIST, 10);
    FileInfo fileInfoA = mTfs.getInfo(fileA);
    FileInfo fileInfoB = mTfs.getInfo(fileB);
    Assert.assertFalse(fileInfoA.getInMemoryPercentage() == 100);
    Assert.assertTrue(fileInfoB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run(new String[] {"load", "/testRoot"});
    fileInfoA = mTfs.getInfo(fileA);
    fileInfoB = mTfs.getInfo(fileB);
    Assert.assertTrue(fileInfoA.getInMemoryPercentage() == 100);
    Assert.assertTrue(fileInfoB.getInMemoryPercentage() == 100);
  }

  @Test
  public void copyFromLocalTest() throws IOException {
    File testDir = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile =
        generateFileContent("/testDir/testFile", BufferUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        BufferUtils.getIncreasingByteArray(10, 20));
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", testFile.getParent(), "/testDir"});
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyFromLocal", testFile.getParent(), "/testDir"}),
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
  public void copyFromLocalTestWithFullURI() throws IOException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String tachyonURI = "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    String cmdOut =
        getCommandOutput(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    Assert.assertEquals(cmdOut, mOutput.toString());
    TachyonFile file = mTfs.open(new TachyonURI("/destFileURI"));
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertEquals(10L, fileInfo.length);
    byte[] read = readContent(file, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyFromLocalFileToDstPathTest() throws IOException {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.run(new String[] {"mkdir", "/dstDir"});
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", localFile.getPath(), "/dstDir"});

    TachyonFile file = mTfs.open(new TachyonURI("/dstDir/testFile"));
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertNotNull(fileInfo);
    byte[] read = readContent(file, data.length);
    Assert.assertEquals(new String(read), dataString);
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
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, bytes);
    mFsShell.copyToLocal(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[bytes];
    fis.read(read);
    fis.close();
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(bytes, read));
  }

  @Test
  public void countNotExistTest() throws IOException {
    int ret = mFsShell.run(new String[] {"count", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void countTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 30);
    mFsShell.run(new String[] {"count", "/testRoot"});
    String expected = "";
    String format = "%-25s%-25s%-15s\n";
    expected += String.format(format, "File Count", "Folder Count", "Total Bytes");
    expected += String.format(format, 3, 2, 60);
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void fileinfoNotExistTest() throws IOException {
    int ret = mFsShell.run(new String[] {"fileinfo", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  /*
   * @Test public void fileInfoTest() throws IOException { TachyonFile file =
   * TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
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
    int ret = mFsShell.run(new String[] {"location", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  /*
   * @Test public void locationTest() throws IOException { TachyonFile file =
   * TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
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
  public void lsrTest() throws IOException {
    FileInfo[] files = new FileInfo[4];

    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA",
        CacheType.CACHE, UnderStorageType.NO_PERSIST, 10);
    files[0] = mTfs.getInfo(fileA);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 20);
    files[1] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir")));
    files[2] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir/testFileB")));
    TachyonFile fileC = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC",
        CacheType.NO_CACHE, UnderStorageType.PERSIST, 30);
    files[3] = mTfs.getInfo(fileC);
    mFsShell.run(new String[] {"lsr", "/testRoot"});
    String expected = "";
    String format = "%-10s%-25s%-15s%-5s\n";
    expected +=
        String.format(format, FormatUtils.getSizeFromBytes(10),
            TFsShell.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/testRoot/testFileA");
    expected +=
        String.format(format, FormatUtils.getSizeFromBytes(0),
            TFsShell.convertMsToDate(files[1].getCreationTimeMs()), "", "/testRoot/testDir");
    expected +=
        String.format(format, FormatUtils.getSizeFromBytes(20),
            TFsShell.convertMsToDate(files[2].getCreationTimeMs()), "In Memory",
            "/testRoot/testDir/testFileB");
    expected +=
        String.format(format, FormatUtils.getSizeFromBytes(30),
            TFsShell.convertMsToDate(files[3].getCreationTimeMs()), "Not In Memory",
            "/testRoot/testFileC");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void lsTest() throws IOException {
    FileInfo[] files = new FileInfo[4];

    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA",
        CacheType.CACHE, UnderStorageType.NO_PERSIST, 10);
    files[0] = mTfs.getInfo(fileA);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 20);
    files[1] = mTfs.getInfo(mTfs.open(new TachyonURI("/testRoot/testDir")));
    TachyonFile fileC = TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC",
        CacheType.NO_CACHE, UnderStorageType.PERSIST, 30);
    files[2] = mTfs.getInfo(fileC);
    mFsShell.run(new String[] {"ls", "/testRoot"});
    String expected = "";
    String format = "%-10s%-25s%-15s%-5s\n";
    expected += String.format(format, FormatUtils.getSizeFromBytes(10),
        TFsShell.convertMsToDate(files[0].getCreationTimeMs()), "In Memory", "/testRoot/testFileA");
    expected += String.format(format, FormatUtils.getSizeFromBytes(0),
        TFsShell.convertMsToDate(files[1].getCreationTimeMs()), "", "/testRoot/testDir");
    expected += String.format(format, FormatUtils.getSizeFromBytes(30),
        TFsShell.convertMsToDate(files[2].getCreationTimeMs()), "Not In Memory",
        "/testRoot/testFileC");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void mkdirComplexPathTest() throws IOException {
    mFsShell.run(new String[] {"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    TachyonFile tFile = mTfs.open(new TachyonURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(
        getCommandOutput(new String[] {"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"}),
        mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }

  @Test
  public void mkdirExistingTest() throws IOException {
    Assert.assertEquals(0, mFsShell.run(new String[] {"mkdir", "/testFile1"}));
    Assert.assertEquals(0, mFsShell.run(new String[] {"mkdir", "/testFile1"}));
  }

  @Test
  public void mkdirInvalidPathTest() throws IOException {
    Assert.assertEquals(-1, mFsShell.run(new String[] {"mkdir", "/test File Invalid Path"}));
  }

  @Test
  public void mkdirShortPathTest() throws IOException {
    mFsShell.run(new String[] {"mkdir", "/root/testFile1"});
    TachyonFile tFile = mTfs.open(new TachyonURI("/root/testFile1"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }

  @Test
  public void mkdirTest() throws IOException {
    String qualifiedPath = "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/root/testFile1";
    mFsShell.run(new String[] {"mkdir", qualifiedPath});
    TachyonFile tFile = mTfs.open(new TachyonURI("/root/testFile1"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(new String[]{"mkdir", qualifiedPath}),
        mOutput.toString());
    Assert.assertTrue(fileInfo.isIsFolder());
  }

  private byte[] readContent(TachyonFile tFile, int length) throws IOException {
    ClientOptions options =
        new ClientOptions.Builder(new TachyonConf()).setCacheType(CacheType.NO_CACHE).build();
    FileInStream tfis = mTfs.getInStream(tFile, options);
    byte[] read = new byte[length];
    tfis.read(read);
    return read;
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run(new String[] {"mkdir", "/test/File1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    mFsShell.rename(new String[] {"rename", "/test", "/test2"});
    toCompare.append(getCommandOutput(new String[] {"mv", "/test", "/test2"}));
    Assert.assertTrue(isFileExist(new TachyonURI("/test2/File1")));
    Assert.assertFalse(isFileExist(new TachyonURI("/test")));
    Assert.assertFalse(isFileExist(new TachyonURI("/test/File1")));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void renameTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run(new String[] {"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertTrue(isFileExist(new TachyonURI("/testFolder1")));
    mFsShell.rename(new String[] {"rename", "/testFolder1", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(isFileExist(new TachyonURI("/testFolder")));
    Assert.assertFalse(isFileExist(new TachyonURI("/testFolder1")));
  }

  @Test
  public void renameToExistingFileTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run(new String[] {"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.run(new String[] {"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertEquals(-1,
        mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder"}));
  }

  @Test
  public void rmNotExistingFileTest() throws IOException {
    mFsShell.run(new String[] {"rm", "/testFile"});
    String expected = "rm: cannot remove '/testFile': No such file or directory\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void rmNotExistingDirTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run(new String[] {"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.run(new String[] {"rm", "/testFolder"});
    toCompare.append("rm: cannot remove a directory, please try rmr <path>\n");
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void rmTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run(new String[] {"mkdir", "/testFolder1/testFolder2"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.run(new String[] {"touch", "/testFolder1/testFolder2/testFile2"});
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(isFileExist(testFolder1));
    Assert.assertTrue(isFileExist(testFolder2));
    Assert.assertTrue(isFileExist(testFile2));
    mFsShell.run(new String[] {"rm", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(isFileExist(testFolder1));
    Assert.assertTrue(isFileExist(testFolder2));
    Assert.assertFalse(isFileExist(testFile2));
  }

  @Test
  public void rmrTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run(new String[] {"mkdir", "/testFolder1/testFolder2"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.run(new String[] {"touch", "/testFolder1/testFolder2/testFile2"});
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(isFileExist(testFolder1));
    Assert.assertTrue(isFileExist(testFolder2));
    Assert.assertTrue(isFileExist(testFile2));
    mFsShell.run(new String[] {"rmr", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(isFileExist(testFolder1));
    Assert.assertTrue(isFileExist(testFolder2));
    Assert.assertFalse(isFileExist(testFile2));
    mFsShell.run(new String[] {"rmr", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"rmr", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertFalse(isFileExist(testFolder1));
    Assert.assertFalse(isFileExist(testFolder2));
    Assert.assertFalse(isFileExist(testFile2));
  }

  @Test
  public void tailEmptyFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/emptyFile", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 0);
    int ret = mFsShell.run(new String[] {"tail", "/emptyFile"});
    Assert.assertEquals(0, ret);
  }

  @Test
  public void tailLargeFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 2048);
    mFsShell.run(new String[] {"tail", "/testFile"});
    byte[] expect = BufferUtils.getIncreasingByteArray(1024, 1024);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailNotExistTest() throws IOException {
    int ret = mFsShell.run(new String[] {"tail", "/testFile"});
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void tailSmallFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 10);
    mFsShell.run(new String[] {"tail", "/testFile"});
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void touchTest() throws IOException {
    String[] argv = new String[] {"touch", "/testFile"};
    mFsShell.run(argv);
    TachyonFile tFile = mTfs.open(new TachyonURI("/testFile"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(fileInfo.isFolder);
  }

  @Test
  public void touchTestWithFullURI() throws IOException {
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
  public void freeTest() throws IOException {
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, "/testFile", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 10);
    mFsShell.run(new String[] {"free", "/testFile"});
    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    CommonUtils.sleepMs(tachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS));
    Assert.assertFalse(mTfs.getInfo(file).getInMemoryPercentage() == 100);
  }

  @Test
  public void duTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testFileB", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testDir/testFileC", CacheType.CACHE,
        UnderStorageType.NO_PERSIST, 30);

    String expected = "";
    // du a non-existing file
    mFsShell.run(new String[] {"du", "/testRoot/noneExisting"});
    expected += "/testRoot/noneExisting does not exist\n";
    // du a file
    mFsShell.run(new String[] {"du", "/testRoot/testFileA"});
    expected += "/testRoot/testFileA is 10 bytes\n";
    // du a folder
    mFsShell.run(new String[] {"du", "/testRoot/testDir"});
    expected += "/testRoot/testDir is 50 bytes\n";

    Assert.assertEquals(expected, mOutput.toString());
  }

  private boolean isFileExist(TachyonURI path) {
    try {
      mTfs.open(path);
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }
}
