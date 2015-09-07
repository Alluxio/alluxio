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
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientBlockInfo;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;
import tachyon.util.io.BufferUtils;

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
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
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
    mFsShell.run(new String[] {"copyFromLocal", testFile.getAbsolutePath(), "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(),
        "/testFile"}), mOutput.toString());
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testFile"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(SIZE_BYTES, tFile.length());
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte[] read = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void loadFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.THROUGH, 10);
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testFile")).isInMemory());
    // Testing loading of a single file
    mFsShell.run(new String[] {"load", "/testFile"});
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testFile")).isInMemory());
  }

  @Test
  public void loadDirTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.THROUGH, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", WriteType.MUST_CACHE, 10);
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testRoot/testFileB")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testRoot/testFileA")).isInMemory());
    // Testing loading of a directory
    mFsShell.run(new String[] {"load", "/testRoot"});
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testRoot/testFileA")).isInMemory());
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testRoot/testFileB")).isInMemory());
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
    mFsShell.run(new String[] {"copyFromLocal", testFile.getParent(), "/testDir"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getParent(),
        "/testDir"}), mOutput.toString());
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/testDir/testFile"));
    TachyonFile tFile2 = mTfs.getFile(new TachyonURI("/testDir/testDirInner/testFile2"));
    Assert.assertNotNull(tFile);
    Assert.assertNotNull(tFile2);
    Assert.assertEquals(10, tFile.length());
    Assert.assertEquals(20, tFile2.length());
    byte[] read = readContent(tFile, 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, read));
    read = readContent(tFile2, 20);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, 20, read));
  }

  @Test
  public void copyFromLocalTestWithFullURI() throws IOException {
    File testFile = generateFileContent("/srcFileURI", BufferUtils.getIncreasingByteArray(10));
    String tachyonURI =
        "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
            + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    mFsShell.run(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    String cmdOut =
        getCommandOutput(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    Assert.assertThat(cmdOut, CoreMatchers.equalTo(mOutput.toString()));
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/destFileURI"));
    Assert.assertThat(tFile.length(), CoreMatchers.equalTo(10L));
    byte[] read = readContent(tFile, 10);
    Assert.assertThat(BufferUtils.equalIncreasingByteArray(10, read), CoreMatchers.equalTo(true));
  }

  @Test
  public void copyFromLocalFileToDstPathTest() throws IOException {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localDir = new File(mLocalTachyonCluster.getTachyonHome() + "/localDir");
    localDir.mkdir();
    File localFile = generateFileContent("/localDir/testFile", data);
    mFsShell.run(new String[] {"mkdir", "/dstDir"});
    mFsShell.run(new String[] {"copyFromLocal", localFile.getPath(), "/dstDir"});

    TachyonFile tFile = mTfs.getFile(new TachyonURI("/dstDir/testFile"));
    Assert.assertNotNull(tFile);
    byte[] read = readContent(tFile, data.length);
    Assert.assertEquals(dataString, new String(read));
  }

  @Test
  public void copyToLocalLargeTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, SIZE_BYTES);
    mFsShell.run(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    fileReadTest("/testFile", SIZE_BYTES);
  }

  @Test
  public void copyToLocalTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    fileReadTest("/testFile", 10);
  }
  
  @Test
  public void copyToLocalDirTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);
    int ret = mFsShell.run(new String[] {"copyToLocal", "/testWildCards/",
        mLocalTachyonCluster.getTachyonHome() + "/testDir"});
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }
  
  private void fileReadTest(String fileName, int size) throws IOException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/" + fileName);
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[size];
    fis.read(read);
    fis.close();
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, read));
  }

  @Test
  public void countNotExistTest() throws IOException {
    int ret = mFsShell.run(new String[] {"count", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void countTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFSTestUtils
        .createByteFile(mTfs, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileB", WriteType.MUST_CACHE, 30);
    mFsShell.run(new String[] {"count", "/testRoot"});
    StringBuilder expected = new StringBuilder(200);
    String format = "%-25s%-25s%-15s\n";
    expected.append(String.format(format, "File Count", "Folder Count", "Total Bytes"));
    expected.append(String.format(format, 3, 2, 60));
    Assert.assertEquals(expected.toString(), mOutput.toString());
  }

  @Test
  public void fileinfoNotExistTest() throws IOException {
    int ret = mFsShell.run(new String[] {"fileinfo", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void fileinfoTest() throws IOException {
    int fileId = TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run(new String[] {"fileinfo", "/testFile"});
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
    int ret = mFsShell.run(new String[] {"location", "/NotExistFile"});
    Assert.assertEquals("/NotExistFile does not exist.\n", mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void locationTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run(new String[] {"location", "/testFile"});
    String locationResult = getLocationResultStr("/testFile");
    Assert.assertEquals(locationResult, mOutput.toString());
  }

  private String getLocationResultStr(String path) throws IOException {
    TachyonURI tUri = new TachyonURI(path);
    TachyonFile tFile = mTfs.getFile(tUri);
    Assert.assertNotNull(tFile);
    List<String> locationsList = tFile.getLocationHosts();
    String[] commandParameters = new String[3 + locationsList.size()];
    commandParameters[0] = "location";
    commandParameters[1] = path;
    commandParameters[2] = String.valueOf(mTfs.getFileId(tUri));
    Iterator<String> iter = locationsList.iterator();
    int i = 3;
    while (iter.hasNext()) {
      commandParameters[i ++] = iter.next();
    }
    return getCommandOutput(commandParameters);
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
    mFsShell.run(new String[] {"ls", "/testRoot"});
    StringBuilder expected = new StringBuilder(200);
    String format = "%-10s%-25s%-15s%-5s\n";
    expected
        .append(String.format(format, FormatUtils.getSizeFromBytes(10),
            TFsShell.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/testRoot/testFileA"));
    expected.append(String.format(format, FormatUtils.getSizeFromBytes(0),
        TFsShell.convertMsToDate(files[1].getCreationTimeMs()), "", "/testRoot/testDir"));
    expected.append(String.format(format, FormatUtils.getSizeFromBytes(30),
        TFsShell.convertMsToDate(files[3].getCreationTimeMs()), "Not In Memory",
        "/testRoot/testFileC"));
    Assert.assertEquals(expected.toString(), mOutput.toString());
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
    mFsShell.run(new String[] {"ls", "/testRoot"});
    StringBuilder expected = new StringBuilder(200);
    String format = "%-10s%-25s%-15s%-5s\n";
    expected
        .append(String.format(format, FormatUtils.getSizeFromBytes(10),
            TFsShell.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/testRoot/testFileA"));
    expected.append(String.format(format, FormatUtils.getSizeFromBytes(0),
        TFsShell.convertMsToDate(files[1].getCreationTimeMs()), "", "/testRoot/testDir"));
    expected.append(String.format(format, FormatUtils.getSizeFromBytes(30),
        TFsShell.convertMsToDate(files[2].getCreationTimeMs()), "Not In Memory",
        "/testRoot/testFileC"));
    Assert.assertEquals(expected.toString(), mOutput.toString());
  }

  @Test
  public void mkdirComplexPathTest() throws IOException {
    mFsShell.run(new String[] {"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir",
        "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"}), mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
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
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/root/testFile1"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  @Test
  public void mkdirTest() throws IOException {
    String qualifiedPath =
        "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
            + mLocalTachyonCluster.getMasterPort() + "/root/testFile1";
    mFsShell.run(new String[] {"mkdir", qualifiedPath});
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
    mFsShell.run(new String[] {"mkdir", "/test/File1"});
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
    mFsShell.run(new String[] {"mkdir", "/testFolder1"});
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
    mFsShell.run(new String[] {"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    mFsShell.run(new String[] {"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert
        .assertEquals(-1, mFsShell.rename(new String[] {"rename", "/testFolder1", "/testFolder"}));
  }

  @Test
  public void rmNotExistingFileTest() throws IOException {
    mFsShell.run(new String[] {"rm", "/testFile"});
    String expected = "/testFile does not exist.\n";
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
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNotNull(mTfs.getFile(testFile2));
    mFsShell.run(new String[] {"rm", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNull(mTfs.getFile(testFile2));
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
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNotNull(mTfs.getFile(testFile2));
    mFsShell.run(new String[] {"rmr", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile(testFolder1));
    Assert.assertNotNull(mTfs.getFile(testFolder2));
    Assert.assertNull(mTfs.getFile(testFile2));
    mFsShell.run(new String[] {"rmr", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"rmr", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNull(mTfs.getFile(testFolder1));
    Assert.assertNull(mTfs.getFile(testFolder2));
    Assert.assertNull(mTfs.getFile(testFile2));
  }

  @Test
  public void tailEmptyFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/emptyFile", WriteType.MUST_CACHE, 0);
    int ret = mFsShell.run(new String[] {"tail", "/emptyFile"});
    Assert.assertEquals(0, ret);
  }

  @Test
  public void tailLargeFileTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 2048);
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
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run(new String[] {"tail", "/testFile"});
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void touchTest() throws IOException {
    String[] argv = new String[] {"touch", "/testFile"};
    mFsShell.run(argv);
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
    mFsShell.run(argv);
    // then
    TachyonFile tFile = mTfs.getFile(new TachyonURI("/destFileURI"));
    Assert.assertThat(tFile, CoreMatchers.notNullValue());
    Assert.assertThat(getCommandOutput(argv), CoreMatchers.equalTo(mOutput.toString()));
    Assert.assertThat(tFile.isFile(), CoreMatchers.equalTo(true));
  }

  @Test
  public void freeTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run(new String[] {"free", "/testFile"});
    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    CommonUtils
        .sleepMs(tachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testFile")).isInMemory());
  }

  @Test
  public void duTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFSTestUtils
        .createByteFile(mTfs, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testRoot/testDir/testDir/testFileC",
        WriteType.MUST_CACHE, 30);

    StringBuilder expected = new StringBuilder(200);
    // du a non-existing file
    mFsShell.run(new String[] {"du", "/testRoot/noneExisting"});
    expected.append("/testRoot/noneExisting does not exist.\n");
    // du a file
    mFsShell.run(new String[] {"du", "/testRoot/testFileA"});
    expected.append("/testRoot/testFileA is 10 bytes\n");
    // du a folder
    mFsShell.run(new String[] {"du", "/testRoot/testDir"});
    expected.append("/testRoot/testDir is 50 bytes\n");

    Assert.assertEquals(expected.toString(), mOutput.toString());
  }

  @Test
  public void catWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    // the expect contents
    byte[] exp1 = BufferUtils.getIncreasingByteArray(10);
    byte[] exp2 = BufferUtils.getIncreasingByteArray(20);
    byte[] exp3 = BufferUtils.getIncreasingByteArray(30);
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = mFsShell.run(new String[] {"cat", "/testWildCards/*/foo*"});
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }

  @Test
  public void freeWildCardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();

    int ret = mFsShell.run(new String[] {"free", "/testWild*/foo/*"});
    CommonUtils.sleepMs(null,
        tachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar1")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar2")).isInMemory());
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")).isInMemory());
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")).isInMemory());

    ret = mFsShell.run(new String[] {"free", "/testWild*/*/"});
    CommonUtils.sleepMs(null,
            tachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")).isInMemory());
  }

  @Test
  public void lsWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    mFsShell.run(new String[] {"ls", "/testWildCards/*/foo*"});
    StringBuilder expected = new StringBuilder(200);
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/foo/foobar1"), 10));
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/foo/foobar2"), 20));
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/bar/foobar3"), 30));
    Assert.assertEquals(expected.toString(), mOutput.toString());

    mFsShell.run(new String[] {"ls", "/testWildCards/*"});
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/foo/foobar1"), 10));
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/foo/foobar2"), 20));
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/bar/foobar3"), 30));
    expected.append(getLsResultStr(new TachyonURI("/testWildCards/foobar4"), 40));
    Assert.assertEquals(expected.toString(), mOutput.toString());
  }

  private String getLsResultStr(TachyonURI tUri, int size) throws IOException {
    String format = "%-10s%-25s%-15s%-5s\n";
    return String.format(format, FormatUtils.getSizeFromBytes(size),
        TFsShell.convertMsToDate(mTfs.getFile(tUri).getCreationTimeMs()), "In Memory",
        tUri.getPath());
  }

  @Test
  public void rmWildCardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    mFsShell.run(new String[] {"rm", "/testWildCards/foo/foo*"});
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar1")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar2")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")));

    mFsShell.run(new String[] {"rm", "/testWildCards/*"});
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/foo")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/bar")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")));
  }

  @Test
  public void rmrWildCardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    mFsShell.run(new String[] {"rmr", "/testWildCards/foo/foo*"});
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar1")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar2")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/foo")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")));

    mFsShell.run(new String[] {"rmr", "/testWildCards/ba*"});
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/bar")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")));

    mFsShell.run(new String[] {"rmr", "/testWildCards/*"});
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/bar")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foo")));
    Assert.assertNull(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")));
  }

  @Test
  public void tailWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    // the expect contents
    byte[] exp1 = BufferUtils.getIncreasingByteArray(10);
    byte[] exp2 = BufferUtils.getIncreasingByteArray(20);
    byte[] exp3 = BufferUtils.getIncreasingByteArray(30);
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = mFsShell.run(new String[] {"tail", "/testWildCards/*/foo*"});
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }

  @Test
  public void fileinfoWildCardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    mFsShell.run(new String[] {"fileinfo", "/testWildCards/*"});
    String res1 = mOutput.toString();
    Assert.assertTrue(res1.contains("/testWildCards/foo"));
    Assert.assertTrue(res1.contains("/testWildCards/bar"));
    Assert.assertTrue(res1.contains("/testWildCards/foobar4"));
    Assert.assertFalse(res1.contains("/testWildCards/foo/foobar1"));
    Assert.assertFalse(res1.contains("/testWildCards/bar/foobar3"));

    mFsShell.run(new String[] {"fileinfo", "/testWildCards/*/foo*"});
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    Assert.assertTrue(res2.contains("/testWildCards/foo/foobar1"));
    Assert.assertTrue(res2.contains("/testWildCards/foo/foobar2"));
    Assert.assertTrue(res2.contains("/testWildCards/bar/foobar3"));
    Assert.assertFalse(res2.contains("/testWildCards/foobar4"));
  }

  @Test
  public void reportWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    mFsShell.run(new String[] {"report", "/testWildCards/*/foo*"});
    StringBuilder expectedOutput = new StringBuilder(200);
    String[] paths =
        {"/testWildCards/foo/foobar1", "/testWildCards/foo/foobar2", "/testWildCards/bar/foobar3",};
    for (String path : paths) {
      expectedOutput.append(path + " with file id " + mTfs.getFileId(new TachyonURI(path))
          + " has reported been report lost.\n");
    }
    Assert.assertEquals(expectedOutput.toString(), mOutput.toString());
  }

  @Test
  public void locationWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);

    mFsShell.run(new String[] {"location", "/testWildCards/*/foo*"});

    String locationResults = "";
    locationResults += getLocationResultStr("/testWildCards/foo/foobar1");
    locationResults += getLocationResultStr("/testWildCards/foo/foobar2");
    locationResults += getLocationResultStr("/testWildCards/bar/foobar3");
    Assert.assertEquals(locationResults, mOutput.toString());
  }

  @Test
  public void loadWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.THROUGH);

    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar1")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar2")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")).isInMemory());

    mFsShell.run(new String[] {"load", "/testWildCards/*/foo*"});

    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar1")).isInMemory());
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testWildCards/foo/foobar2")).isInMemory());
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testWildCards/bar/foobar3")).isInMemory());
    Assert.assertFalse(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")).isInMemory());

    mFsShell.run(new String[] {"load", "/testWildCards/*"});
    Assert.assertTrue(mTfs.getFile(new TachyonURI("/testWildCards/foobar4")).isInMemory());
  }
  
  @Test
  public void copyFromLocalWildcardTest() throws IOException {
    TFsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    int ret = mFsShell.run(new String[] {"copyFromLocal", 
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testDir"});
    Assert.assertEquals(0, ret);
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar1")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar2")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar3")));
  }

  @Test
  public void copyFromLocalWildcardExistingDirTest() throws IOException {
    TFsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    mTfs.mkdir(new TachyonURI("/testDir"));
    int ret = mFsShell.run(new String[] {"copyFromLocal", 
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testDir"});
    Assert.assertEquals(0, ret);
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar1")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar2")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar3")));
  }
  
  @Test
  public void copyFromLocalWildcardNotDirTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);
    int ret = mFsShell.run(new String[] {"copyFromLocal", 
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*/foo*", "/testWildCards/foobar4"});
    Assert.assertEquals(-1, ret);
  }
  
  @Test
  public void copyFromLocalWildcardHierTest() throws IOException {
    TFsShellUtilsTest.resetLocalFileHierarchy(mLocalTachyonCluster);
    int ret = mFsShell.run(new String[] {"copyFromLocal", 
        mLocalTachyonCluster.getTachyonHome() + "/testWildCards/*", "/testDir"});
    
    mFsShell.run(new String[] {"ls", "/testDir"});
    Assert.assertEquals(0, ret);
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foo/foobar1")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foo/foobar2")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/bar/foobar3")));
    Assert.assertNotNull(mTfs.getFile(new TachyonURI("/testDir/foobar4")));
  }
  
  @Test
  public void copyToLocalWildcardTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);
    int ret = mFsShell.run(new String[] {"copyToLocal", "/testWildCards/*/foo*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir"});
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }
  
  @Test
  public void copyToLocalWildcardExistingDirTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);
    
    new File(mLocalTachyonCluster.getTachyonHome() + "/testDir").mkdir();
    
    int ret = mFsShell.run(new String[] {"copyToLocal", "/testWildCards/*/foo*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir"});
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }
  
  @Test
  public void copyToLocalWildcardNotDirTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);
    new File(mLocalTachyonCluster.getTachyonHome() + "/testDir").mkdir();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testDir/testFile").createNewFile();
    
    int ret = mFsShell.run(new String[] {"copyToLocal", "/testWildCards/*/foo*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir/testFile"});
    Assert.assertEquals(-1, ret);
  }
  
  @Test
  public void copyToLocalWildcardHierTest() throws IOException {
    TFsShellUtilsTest.resetTachyonFileHierarchy(mTfs, WriteType.MUST_CACHE);
    int ret = mFsShell.run(new String[] {"copyToLocal", "/testWildCards/*",
        mLocalTachyonCluster.getTachyonHome() + "/testDir"});
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }
}
