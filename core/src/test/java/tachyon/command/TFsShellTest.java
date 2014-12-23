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
package tachyon.command;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientBlockInfo;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  private static final int SIZE_BYTES = Constants.MB * 1;
  private static LocalTachyonCluster CLUSTER = null;
  private static TachyonFS TFS = null;
  private static TFsShell SHELL = null;
  private ByteArrayOutputStream mOutput = null;
  private PrintStream mNewOutput = null;
  private PrintStream mOldOutput = null;

  @BeforeClass
  public static final void beforeClass() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    CLUSTER = new LocalTachyonCluster(SIZE_BYTES * 10);
    CLUSTER.start();
    TFS = CLUSTER.getClient();
    SHELL = new TFsShell();
  }

  @Before
  public final void before() {
    mOutput = new ByteArrayOutputStream();
    mNewOutput = new PrintStream(mOutput);
    mOldOutput = System.out;
    System.setOut(mNewOutput);
  }

  @AfterClass
  public static final void afterClass() throws Exception {
    SHELL.close();
    CLUSTER.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @After
  public final void after() {
    System.setOut(mOldOutput);
  }

  @Test
  public void catDirectoryTest() throws IOException {
    final String path = TestUtils.uniqFile();
    String[] command = new String[] {"mkdir", path};
    SHELL.mkdir(command);
    int ret = SHELL.cat(new String[] {"cat", path});
    Assert.assertEquals(-1, ret);
    String expected = getCommandOutput(command);
    expected += path + " is not a file.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void catNotExistTest() throws IOException {
    final String path = TestUtils.uniqFile();
    int ret = SHELL.cat(new String[] {"cat", path});
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void catTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 10);
    SHELL.cat(new String[]{"cat", path});
    byte expect[] = TestUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void copyFromLocalLargeTest() throws IOException {
    final String path = TestUtils.uniqFile();
    File testFile = new File(CLUSTER.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte toWrite[] = TestUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    SHELL.copyFromLocal(new String[]{"copyFromLocal", testFile.getAbsolutePath(), path});
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(),
        path}), mOutput.toString());
    TachyonFile tFile = TFS.getFile(new TachyonURI(path));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(SIZE_BYTES, tFile.length());
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte read[] = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void copyFromLocalTest() throws IOException {
    final String path = TestUtils.uniqFile();
    File testDir = new File(CLUSTER.getTachyonHome() + "/testDir");
    testDir.mkdir();
    File testDirInner = new File(CLUSTER.getTachyonHome() + "/testDir/testDirInner");
    testDirInner.mkdir();
    File testFile = generateFileContent("/testDir/testFile", TestUtils.getIncreasingByteArray(10));
    generateFileContent("/testDir/testDirInner/testFile2",
        TestUtils.getIncreasingByteArray(10, 20));
    SHELL.copyFromLocal(new String[]{"copyFromLocal", testFile.getParent(), "/testDir"});
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getParent(),
        "/testDir"}), mOutput.toString());
    TachyonFile tFile = TFS.getFile(new TachyonURI("/testDir/testFile"));
    TachyonFile tFile2 = TFS.getFile(new TachyonURI("/testDir/testDirInner/testFile2"));
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
    final String path = TestUtils.uniqFile();
    File testFile = generateFileContent("/srcFileURI", TestUtils.getIncreasingByteArray(10));
    String tachyonURI =
        "tachyon://" + CLUSTER.getMasterHostname() + ":"
            + CLUSTER.getMasterPort() + "/destFileURI";
    // when
    SHELL.copyFromLocal(new String[]{"copyFromLocal", testFile.getPath(), tachyonURI});
    String cmdOut =
        getCommandOutput(new String[] {"copyFromLocal", testFile.getPath(), tachyonURI});
    // then
    assertThat(cmdOut, equalTo(mOutput.toString()));
    TachyonFile tFile = TFS.getFile(new TachyonURI("/destFileURI"));
    assertThat(tFile.length(), equalTo(10L));
    byte[] read = readContent(tFile, 10);
    assertThat(TestUtils.equalIncreasingByteArray(10, read), equalTo(true));
  }

  @Test
  public void copyToLocalLargeTest() throws IOException {
    final String path = TestUtils.uniqFile();
    final File tmp = Files.createTempDir();
    final File tmpFile = new File(tmp, "testFile");
    TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, SIZE_BYTES);
    SHELL.copyToLocal(new String[]{"copyToLocal", path, tmpFile.getAbsolutePath()});
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyToLocal", path, tmpFile.getAbsolutePath()}),
        mOutput.toString());
    FileInputStream fis = new FileInputStream(tmpFile);
    byte read[] = new byte[SIZE_BYTES];
    fis.read(read);
    fis.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }

  @Test
  public void copyToLocalTest() throws IOException {
    final String path = TestUtils.uniqFile();
    final File tmp = Files.createTempDir();
    final File tmpFile = new File(tmp, "testFile");
    // create new file
    TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 10);
    SHELL.copyToLocal(new String[]{"copyToLocal", path, tmpFile.getAbsolutePath()});
    Assert.assertEquals(
        getCommandOutput(new String[] {"copyToLocal", path, tmpFile.getAbsolutePath()}),
        mOutput.toString());
    FileInputStream fis = new FileInputStream(tmpFile);
    byte read[] = new byte[10];
    fis.read(read);
    fis.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void countTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TestUtils.createByteFile(TFS, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TestUtils.createByteFile(TFS, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    TestUtils.createByteFile(TFS, "/testRoot/testFileB", WriteType.MUST_CACHE, 30);
    SHELL.count(new String[]{"count", "/testRoot"});
    String expected = "";
    String format = "%-25s%-25s%-15s\n";
    expected += String.format(format, "File Count", "Folder Count", "Total Bytes");
    expected += String.format(format, 3, 2, 60);
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void fileinfoTest() throws IOException {
    final String path = TestUtils.uniqFile();
    int fileId = TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 10);
    SHELL.fileinfo(new String[]{"fileinfo", path});
    TachyonFile tFile = TFS.getFile(new TachyonURI(path));
    Assert.assertNotNull(tFile);
    List<ClientBlockInfo> blocks = TFS.getFileBlocks(fileId);
    String[] commandParameters = new String[3 + blocks.size()];
    commandParameters[0] = "fileinfo";
    commandParameters[1] = path;
    commandParameters[2] = String.valueOf(fileId);
    Iterator<ClientBlockInfo> iter = blocks.iterator();
    int i = 3;
    while (iter.hasNext()) {
      commandParameters[i ++] = iter.next().toString();
    }
    Assert.assertEquals(getCommandOutput(commandParameters), mOutput.toString());
  }

  private File generateFileContent(String path, byte toWrite[]) throws IOException,
      FileNotFoundException {
    File testFile = new File(CLUSTER.getTachyonHome() + path);
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
      } else if (cmd.equals("rm")) {
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
  public void locationTest() throws IOException {
    final String path = TestUtils.uniqFile();
    int fileId = TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 10);
    SHELL.location(new String[]{"location", path});
    TachyonFile tFile = TFS.getFile(new TachyonURI(path));
    Assert.assertNotNull(tFile);
    List<String> locationsList = tFile.getLocationHosts();
    String[] commandParameters = new String[3 + locationsList.size()];
    commandParameters[0] = "location";
    commandParameters[1] = path;
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
        TestUtils.createByteFile(TFS, "/lsrTest/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFile[] files = new TachyonFile[4];
    files[0] = TFS.getFile(fileIdA);
    TestUtils.createByteFile(TFS, "/lsrTest/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    files[1] = TFS.getFile(new TachyonURI("/lsrTest/testRoot/testDir"));
    files[2] = TFS.getFile(new TachyonURI("/lsrTest/testRoot/testDir/testFileB"));
    int fileIdC =
        TestUtils.createByteFile(TFS, "/lsrTest/testRoot/testFileC", WriteType.THROUGH, 30);
    files[3] = TFS.getFile(fileIdC);
    SHELL.ls(new String[]{"count", "/lsrTest/testRoot"});
    String expected = "";
    String format = "%-10s%-25s%-15s%-5s\n";
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(10),
            CommonUtils.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/lsrTest/testRoot/testFileA");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(0),
            CommonUtils.convertMsToDate(files[1].getCreationTimeMs()), "",
            "/lsrTest/testRoot/testDir");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(30),
            CommonUtils.convertMsToDate(files[3].getCreationTimeMs()), "Not In Memory",
            "/lsrTest/testRoot/testFileC");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void lsTest() throws IOException {
    int fileIdA =
        TestUtils.createByteFile(TFS, "/lsTest/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    TachyonFile[] files = new TachyonFile[3];
    files[0] = TFS.getFile(fileIdA);
    TestUtils.createByteFile(TFS, "/lsTest/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    files[1] = TFS.getFile(new TachyonURI("/lsTest/testRoot/testDir"));
    int fileIdC =
        TestUtils.createByteFile(TFS, "/lsTest/testRoot/testFileC", WriteType.THROUGH, 30);
    files[2] = TFS.getFile(fileIdC);
    SHELL.ls(new String[]{"count", "/lsTest/testRoot"});
    String expected = "";
    String format = "%-10s%-25s%-15s%-5s\n";
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(10),
            CommonUtils.convertMsToDate(files[0].getCreationTimeMs()), "In Memory",
            "/lsTest/testRoot/testFileA");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(0),
            CommonUtils.convertMsToDate(files[1].getCreationTimeMs()), "",
            "/lsTest/testRoot/testDir");
    expected +=
        String.format(format, CommonUtils.getSizeFromBytes(30),
            CommonUtils.convertMsToDate(files[2].getCreationTimeMs()), "Not In Memory",
            "/lsTest/testRoot/testFileC");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void mkdirComplexPathTest() throws IOException {
    final String path = TestUtils.uniqFile();
    SHELL.mkdir(new String[]{"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    TachyonFile tFile = TFS.getFile(new TachyonURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir",
        "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"}), mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  @Test
  public void mkdirExistingTest() throws IOException {
    final String path = TestUtils.uniqFile();
    Assert.assertEquals(0, SHELL.mkdir(new String[]{"mkdir", "/testFile1"}));
    Assert.assertEquals(0, SHELL.mkdir(new String[] {"mkdir", "/testFile1"}));
  }

  @Test(expected = IOException.class)
  public void mkdirInvalidPathTest() throws IOException {
    final String path = TestUtils.uniqFile();
    SHELL.mkdir(new String[]{"mkdir", "/test File Invalid Path"});
  }

  @Test
  public void mkdirShortPathTest() throws IOException {
    final String path = TestUtils.uniqFile();
    SHELL.mkdir(new String[]{"mkdir", "/root/testFile1"});
    TachyonFile tFile = TFS.getFile(new TachyonURI("/root/testFile1"));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  @Test
  public void mkdirTest() throws IOException {
    final String path = TestUtils.uniqFile();
    String qualifiedPath =
        "tachyon://" + NetworkUtils.getLocalHostName() + ":" + CLUSTER.getMasterPort()
            + "/root/testFile1";
    SHELL.mkdir(new String[]{"mkdir", qualifiedPath});
    TachyonFile tFile = TFS.getFile(new TachyonURI("/root/testFile1"));
    Assert.assertNotNull(tFile);
    Assert
        .assertEquals(getCommandOutput(new String[] {"mkdir", qualifiedPath}), mOutput.toString());
    Assert.assertTrue(tFile.isDirectory());
  }

  private byte[] readContent(TachyonFile tFile, int length) throws IOException {
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte read[] = new byte[length];
    tfis.read(read);
    return read;
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    final String path = TestUtils.uniqFile();
    StringBuilder toCompare = new StringBuilder();
    SHELL.mkdir(new String[]{"mkdir", "/test/File1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/test/File1"}));
    SHELL.rename(new String[]{"rename", "/test", "/test2"});
    toCompare.append(getCommandOutput(new String[] {"mv", "/test", "/test2"}));
    Assert.assertNotNull(TFS.getFile(new TachyonURI("/test2/File1")));
    Assert.assertNull(TFS.getFile(new TachyonURI("/test")));
    Assert.assertNull(TFS.getFile(new TachyonURI("/test/File1")));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void renameTest() throws IOException {
    final String path = TestUtils.uniqFile();
    StringBuilder toCompare = new StringBuilder();
    SHELL.mkdir(new String[]{"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert.assertNotNull(TFS.getFile(new TachyonURI("/testFolder1")));
    SHELL.rename(new String[]{"rename", "/testFolder1", "/testFolder"});
    toCompare.append(getCommandOutput(new String[]{"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(TFS.getFile(new TachyonURI("/testFolder")));
    Assert.assertNull(TFS.getFile(new TachyonURI("/testFolder1")));
  }

  @Test
  public void renameToExistingFileTest() throws IOException {
    final String path = TestUtils.uniqFile();
    StringBuilder toCompare = new StringBuilder();
    SHELL.mkdir(new String[]{"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder"}));
    SHELL.mkdir(new String[]{"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1"}));
    Assert
        .assertEquals(-1, SHELL.rename(new String[]{"rename", "/testFolder1", "/testFolder"}));
  }

  @Test
  public void rmNotExistingFileTest() throws IOException {
    final String path = TestUtils.uniqFile();
    Assert.assertEquals(0, SHELL.rm(new String[]{"rm", path}));
  }

  @Test
  public void rmTest() throws IOException {
    final String path = TestUtils.uniqFile();
    StringBuilder toCompare = new StringBuilder();
    SHELL.mkdir(new String[]{"mkdir", "/testFolder1/testFolder2/testFile2"});
    toCompare
        .append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertNotNull(TFS.getFile(testFolder1));
    Assert.assertNotNull(TFS.getFile(testFolder2));
    Assert.assertNotNull(TFS.getFile(testFile2));
    SHELL.rm(new String[]{"rm", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(new String[]{"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(TFS.getFile(testFolder1));
    Assert.assertNotNull(TFS.getFile(testFolder2));
    Assert.assertNull(TFS.getFile(testFile2));
    SHELL.rm(new String[]{"rm", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNull(TFS.getFile(testFolder1));
    Assert.assertNull(TFS.getFile(testFolder2));
    Assert.assertNull(TFS.getFile(testFile2));
  }

  @Test
  public void tailLargeFileTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 2048);
    SHELL.tail(new String[]{"tail", path});
    byte expect[] = TestUtils.getIncreasingByteArray(1024, 1024);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailNotExistTest() throws IOException {
    final String path = TestUtils.uniqFile();
    int ret = SHELL.tail(new String[] {"tail", path});
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void tailSmallFileTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 10);
    SHELL.tail(new String[]{"tail", path});
    byte expect[] = TestUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void touchTest() throws IOException {
    final String path = TestUtils.uniqFile();
    String[] argv = new String[] {"touch", path};
    SHELL.touch(argv);
    TachyonFile tFile = TFS.getFile(new TachyonURI(path));
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertTrue(tFile.isFile());
  }

  @Test
  public void touchTestWithFullURI() throws IOException {
    final String path = TestUtils.uniqFile();
    String tachyonURI =
        "tachyon://" + CLUSTER.getMasterHostname() + ":"
            + CLUSTER.getMasterPort() + path;
    // when
    String[] argv = new String[] {"touch", tachyonURI};
    SHELL.touch(argv);
    // then
    TachyonFile tFile = TFS.getFile(new TachyonURI(path));
    assertThat(tFile, notNullValue());
    assertThat(getCommandOutput(argv), equalTo(mOutput.toString()));
    assertThat(tFile.isFile(), equalTo(true));
  }

  @Test
  public void freeTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TestUtils.createByteFile(TFS, path, WriteType.MUST_CACHE, 10);
    SHELL.free(new String[]{"free", path});
    CommonUtils.sleepMs(null, WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS * 2);;
    Assert.assertFalse(TFS.getFile(new TachyonURI(path)).isInMemory());;
  }
}
