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
import java.util.Iterator;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientBlockInfo;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  private static final int SIZE_BYTES = Constants.MB * 10;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
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
  public final void before() throws IOException {
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
    mFsShell.mkdir(command);
    int ret = mFsShell.cat(new String[] {"cat", "/testDir"});
    Assert.assertEquals(-1, ret);
    String expected = getCommandOutput(command);
    expected += "/testDir is not a file.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void catNotExistTest() throws IOException {
    int ret = mFsShell.cat(new String[] {"cat", "/testFile"});
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void catTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.cat(new String[] {"cat", "/testFile"});
    byte[] expect = TestUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void copyFromLocalLargeTest() throws IOException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = TestUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    mFsShell
        .copyFromLocal(new String[] {"copyFromLocal", testFile.getAbsolutePath(), "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(),
        "/testFile"}), mOutput.toString());
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testFile"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(SIZE_BYTES, tFile.length());
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte[] read = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void copyFromLocalTest() throws IOException {
    File testDir = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(mLocalTachyonCluster.getTachyonHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile = generateFileContent("/testDir/testFile", TestUtils.getIncreasingByteArray(10));
    generateFileContent(
        "/testDir/testDirInner/testFile2", TestUtils.getIncreasingByteArray(10, 20));
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", testFile.getParent(), "/testDir"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getParent(),
        "/testDir"}), mOutput.toString());
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testDir/testFile"));
    TachyonFile tFile2 = mTfs.getFile(new TachyonURI("/testDir/testDirInner/testFile2"));
    Assert.assertNotNull(tFile);
    Assert.assertNotNull(tFile2);
    Assert.assertEquals(10, tFile.length());
    Assert.assertEquals(20, tFile2.length());
    byte[] read = readContent(tFile, 10);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, read));
    read = readContent(tFile2, 20);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, 20, read));
  }

  @Test
  public void copyFromLocalTestWithFullURI() throws IOException {
    File testFile = generateFileContent("/srcFileURI", TestUtils.getIncreasingByteArray(10));
    String tachyonURI =
        "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
            + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    String cmdOut =
        getCommandOutput(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    Assert.assertThat(cmdOut, CoreMatchers.equalTo(mOutput.toString()));
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/destFileURI"));
    Assert.assertThat(tFile.length(), CoreMatchers.equalTo(10L));
    byte[] read = readContent(tFile, 10);
    Assert.assertThat(TestUtils.equalIncreasingByteArray(10, read), CoreMatchers.equalTo(true));
  }

  @Test
  public void copyFromLocalFileToDstPathTest() throws IOException {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.mkdir(new String[] {"mkdir", "/dstDir"});
    mFsShell.copyFromLocal(new String[] {"copyFromLocal", localFile.getPath(), "/dstDir"});

    TachyonFile tFile = mTfs.getFile(new TachyonURI("/dstDir/testFile"));
    Assert.assertNotNull(tFile);
    byte[] read = readContent(tFile, data.length);
    Assert.assertEquals(new String(read), dataString);
  }

  @Test
  public void copyToLocalLargeTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, SIZE_BYTES);
    mFsShell.copyToLocal(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[SIZE_BYTES];
    fis.read(read);
    fis.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void copyToLocalTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.copyToLocal(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[10];
    fis.read(read);
    fis.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void countNotExistTest() throws IOException {
    int ret = mFsShell.count(new String[] {"count", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void countTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFSTestUtils
        .createByteFile(mTfs, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", WriteType.MUST_CACHE, 30);
    mFsShell.count(new String[] {"count", "/testRoot"});
    String expected = "";
    String format = "%-25s%-25s%-15s\n";
    expected += String.format(format, "File Count", "Folder Count", "Total Bytes");
    expected += String.format(format, 3, 2, 60);
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void fileinfoNotExistTest() throws IOException {
    int ret = mFsShell.fileinfo(new String[] {"fileinfo", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void fileinfoTest() throws IOException {
    int fileId = TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.fileinfo(new String[] {"fileinfo", "/testFile"});
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testFile"));
    Assert.assertNotNull(tFile);
    List<ClientBlockInfo> blocks = mTfs.getFileBlocks(fileId);
    String[] commandParameters = new String[3 + blocks.size()];
    commandParameters[0] = "fileinfo";
    commandParameters[1] = "/testFile";
    commandParameters[2] = String.valueOf(fileId);
    Iterator<ClientBlockInfo> iter = blocks.iterator();
    int i = 3;
    while (iter.hasNext()) {
      commandParameters[i ++] = iter.next().toString();
    }
    Assert.assertEquals(getCommandOutput(commandParameters), mOutput.toString());
  }

  private File generateFileContent(String path, byte[] toWrite) throws IOException,
      FileNotFoundException {
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
    int ret = mFsShell.location(new String[] {"location", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void locationTest() throws IOException {
    int fileId = TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.location(new String[] {"location", "/testFile"});
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testFile"));
    Assert.assertNotNull(tFile);
    List<String> locationsList = tFile.getLocationHosts();
    String[] commandParameters = new String[3 + locationsList.size()];
    commandParameters[0] = "location";
    commandParameters[1] = "/testFile";
    commandParameters[2] = String.valueOf(fileId);
    Iterator<String> iter = locationsList.iterator();
    int i = 3;
    while (iter.hasNext()) {
      commandParameters[i ++] = iter.next();
    }
    Assert.assertEquals(getCommandOutput(commandParameters), mOutput.toString());
  }

  @Test
  public void lsrTest() throws IOException {
    int fileIdA =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFile[] files = new TachyonFile[4];
    files[0] = mTfs.getFile(fileIdA);
    TachyonFSTestUtils
        .createByteFile(mTfs, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    files[1] = mTfs.getFile(new TachyonURI("/testRoot/testDir"));
    files[2] = mTfs.getFile(new TachyonURI("/testRoot/testDir/testFileB"));
    int fileIdC =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC", WriteType.THROUGH, 30);
    files[3] = mTfs.getFile(fileIdC);
    mFsShell.ls(new String[] {"count", "/testRoot"});
    String expected = "";
    String format = "%-10s%-25s%-15s%-5s\n";
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(10),
            TFsShell.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/testRoot/testFileA");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(0),
            TFsShell.convertMsToDate(files[1].getCreationTimeMs()), "", "/testRoot/testDir");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(30),
            TFsShell.convertMsToDate(files[3].getCreationTimeMs()), "Not In Memory",
            "/testRoot/testFileC");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void lsTest() throws IOException {
    int fileIdA =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFile[] files = new TachyonFile[3];
    files[0] = mTfs.getFile(fileIdA);
    TachyonFSTestUtils
        .createByteFile(mTfs, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    files[1] = mTfs.getFile(new TachyonURI("/testRoot/testDir"));
    int fileIdC =
        TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileC", WriteType.THROUGH, 30);
    files[2] = mTfs.getFile(fileIdC);
    mFsShell.ls(new String[] {"count", "/testRoot"});
    String expected = "";
    String format = "%-10s%-25s%-15s%-5s\n";
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(10),
            TFsShell.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/testRoot/testFileA");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(0),
            TFsShell.convertMsToDate(files[1].getCreationTimeMs()), "", "/testRoot/testDir");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(30),
            TFsShell.convertMsToDate(files[2].getCreationTimeMs()), "Not In Memory",
            "/testRoot/testFileC");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void mkdirComplexPathTest() throws IOException {
    mFsShell.mkdir(new String[] {"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir",
        "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"}), mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  @Test
  public void mkdirExistingTest() throws IOException {
    Assert.assertEquals(0, mFsShell.mkdir(new String[] {"mkdir", "/testFile1"}));
    Assert.assertEquals(0, mFsShell.mkdir(new String[] {"mkdir", "/testFile1"}));
  }

  @Test(expected = IOException.class)
  public void mkdirInvalidPathTest() throws IOException {
    mFsShell.mkdir(new String[] {"mkdir", "/test File Invalid Path"});
  }

  @Test
  public void mkdirShortPathTest() throws IOException {
    mFsShell.mkdir(new String[] {"mkdir", "/root/testFile1"});
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/root/testFile1"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  @Test
  public void mkdirTest() throws IOException {
    String qualifiedPath =
        "tachyon://" + NetworkUtils.getLocalHostName(Constants.DEFAULT_HOST_RESOLUTION_TIMEOUT_MS)
            + ":" + mLocalTachyonCluster.getMasterPort() + "/root/testFile1";
    mFsShell.mkdir(new String[] {"mkdir", qualifiedPath});
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/root/testFile1"));
    Assert.assertNotNull(tFile);
    Assert
        .assertEquals(getCommandOutput(new String[] {"mkdir", qualifiedPath}), mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  private byte[] readContent(TachyonFile tFile, int length) throws IOException {
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte[] read = new byte[length];
    tfis.read(read);
    return read;
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[] {"mkdir", "/test/File1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    mFsShell.rename(new String[] {"rename", "/test", "/test2"});
    toCompare.append(getCommandOutput(new String[] {"mv", "/test", "/test2"}));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/test2/File1")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/test")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/test/File1")));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void renameTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[] {"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testFolder1")));
    mFsShell.rename(new String[] {"rename", "/testFolder1", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testFolder")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testFolder1")));
  }

  @Test
  public void renameToExistingFileTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[] {"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.mkdir(new String[] {"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert
        .assertEquals(-1, mFsShell.rename(new String[] {"rename", "/testFolder1", "/testFolder"}));
  }

  @Test
  public void rmNotExistingFileTest() throws IOException {
    mFsShell.rm(new String[] {"rm", "/testFile"});
    String expected = "rm: cannot remove '/testFile': No such file or directory\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void rmNotExistingDirTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[] {"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.rm(new String[] {"rm", "/testFolder"});
    toCompare.append("rm: cannot remove a directory, please try rmr <path>\n");
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void rmTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[] {"mkdir", "/testFolder1/testFolder2"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.touch(new String[] {"touch", "/testFolder1/testFolder2/testFile2"});
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNotNull(mTfs.getFile(testFile2));
    mFsShell.rm(new String[] {"rm", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNull(mTfs.getFile(testFile2));
  }

  @Test
  public void rmrTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[] {"mkdir", "/testFolder1/testFolder2"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.touch(new String[] {"touch", "/testFolder1/testFolder2/testFile2"});
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNotNull(mTfs.getFile(testFile2));
    mFsShell.rmr(new String[] {"rmr", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNull(mTfs.getFile(testFile2));
    mFsShell.rmr(new String[] {"rmr", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"rmr", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNull(mTfs.getFile(testFolder1));
    Assert.assertNull(mTfs.getFile(testFolder2));
    Assert.assertNull(mTfs.getFile(testFile2));
  }

  @Test
  public void tailEmptyFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/emptyFile", WriteType.MUST_CACHE, 0);
    int ret = mFsShell.tail(new String[] {"tail", "/emptyFile"});
    Assert.assertEquals(0, ret);
  }

  @Test
  public void tailLargeFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 2048);
    mFsShell.tail(new String[] {"tail", "/testFile"});
    byte[] expect = TestUtils.getIncreasingByteArray(1024, 1024);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailNotExistTest() throws IOException {
    int ret = mFsShell.tail(new String[] {"tail", "/testFile"});
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void tailSmallFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.tail(new String[] {"tail", "/testFile"});
    byte[] expect = TestUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void touchTest() throws IOException {
    String[] argv = new String[] {"touch", "/testFile"};
    mFsShell.touch(argv);
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testFile"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertTrue(tFile.isFile());
  }

  @Test
  public void touchTestWithFullURI() throws IOException {
    String tachyonURI =
        "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
            + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    String[] argv = new String[] {"touch", tachyonURI};
    mFsShell.touch(argv);
    // then
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/destFileURI"));
    Assert.assertThat(tFile, CoreMatchers.notNullValue());
    Assert.assertThat(getCommandOutput(argv), CoreMatchers.equalTo(mOutput.toString()));
    Assert.assertThat(tFile.isFile(), CoreMatchers.equalTo(true));
  }

  @Test
  public void freeTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.free(new String[] {"free", "/testFile"});
    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    CommonUtils.sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(tachyonConf) * 2 + 10);
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testFile")).isInMemory());
  }

  @Test
  public void duTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFSTestUtils
        .createByteFile(mTfs, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testDir/testFileC",
        WriteType.MUST_CACHE, 30);

    String expected = "";
    // du a non-existing file
    mFsShell.du(new String[] {"du", "/testRoot/noneExisting"});
    expected += "/testRoot/noneExisting does not exist\n";
    // du a file
    mFsShell.du(new String[] {"du", "/testRoot/testFileA"});
    expected += "/testRoot/testFileA is 10 bytes\n";
    // du a folder
    mFsShell.du(new String[] {"du", "/testRoot/testDir"});
    expected += "/testRoot/testDir is 50 bytes\n";

    Assert.assertEquals(expected, mOutput.toString());
  }
}
