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
package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.table.RawTable;
import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Unit tests on TachyonClient.
 */
public class TachyonFSTest {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static final int SLEEP_MS = WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS * 2 + 10;
  private static LocalTachyonCluster CLUSTER = null;
  private static TachyonFS TFS = null;

  @AfterClass
  public static final void after() throws Exception {
    CLUSTER.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.max.columns");
  }

  @BeforeClass
  public static final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    System.setProperty("tachyon.max.columns", "257");
    CLUSTER = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    CLUSTER.start();
    TFS = CLUSTER.getClient();
  }

  @After
  public void cleanup() throws IOException {
    // clean up the cluster at the end of each test
    // this way tests that check memory should pass
    for (ClientFileInfo p : TFS.listStatus(new TachyonURI("/"))) {
      TFS.delete(new TachyonURI(p.getPath()), true);
    }
  }

  @Test
  public void getRootTest() throws IOException {
    Assert.assertEquals(1, TFS.getFileId(new TachyonURI(TachyonURI.SEPARATOR)));
  }

  @Test
  public void createFileTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TFS.createFile(new TachyonURI(path + "/root/testFile1"));
    TFS.createFile(new TachyonURI(path + "/root/testFile2"));
    TFS.createFile(new TachyonURI(path + "/root/testFile3"));
  }

  @Test
  public void createFileTest2() throws IOException {
    final String path = TestUtils.uniqFile();
    for (int k = 1; k < 4; k ++) {
      TachyonURI uri = new TachyonURI(path + "/root/testFile" + k);
      TFS.createFile(uri);
      Assert.assertTrue(TFS.exist(uri));
    }
  }

  @Test
  public void createFileWithUfsFileTest() throws IOException {
    String tempFolder = CLUSTER.getTempFolderInUnderFs();
    UnderFileSystem underFs = UnderFileSystem.get(tempFolder);
    OutputStream os = underFs.create(tempFolder + "/temp", 100);
    os.close();
    TachyonURI uri = new TachyonURI("/abc");
    TFS.createFile(uri, new TachyonURI(tempFolder + "/temp"));
    Assert.assertTrue(TFS.exist(uri));
    Assert.assertEquals(tempFolder + "/temp", TFS.getFile(uri).getUfsPath());
  }

  @Test(expected = IOException.class)
  public void createFileWithFileAlreadyExistExceptionTest() throws IOException {
    final String path = TestUtils.uniqFile();
    TFS.createFile(new TachyonURI(path + "/root/testFile1"));
    TFS.createFile(new TachyonURI(path + "/root/testFile1"));
  }

  @Test(expected = IOException.class)
  public void createFileWithInvalidPathExceptionTest() throws IOException {
    TFS.createFile(new TachyonURI("root/testFile1"));
  }

  @Test
  public void createRawTableTestEmptyMetadata() throws IOException {
    int fileId = TFS.createRawTable(new TachyonURI("/tables/table1"), 20);
    RawTable table = TFS.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

    table = TFS.getRawTable(new TachyonURI("/tables/table1"));
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
  }

  @Test
  public void createRawTableTestWithMetadata() throws IOException {
    final String path = TestUtils.uniqFile();
    TachyonURI uri = new TachyonURI(path + "/tables/table1");
    int fileId = TFS.createRawTable(uri, 20, TestUtils.getIncreasingByteBuffer(9));
    RawTable table = TFS.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals(path + "/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());

    table = TFS.getRawTable(uri);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals(path + "/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());
  }

  @Test(expected = IOException.class)
  public void createRawTableWithFileAlreadyExistExceptionTest() throws IOException {
    TachyonURI uri = new TachyonURI("/table");
    TFS.createRawTable(uri, 20);
    TFS.createRawTable(uri, 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithInvalidPathExceptionTest1() throws IOException {
    TFS.createRawTable(new TachyonURI("tables/table1"), 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithInvalidPathExceptionTest2() throws IOException {
    TFS.createRawTable(new TachyonURI("/tab les/table1"), 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest1() throws IOException {
    String maxColumnsProp = System.getProperty("tachyon.max.columns");

    Assert.assertEquals(Integer.parseInt(maxColumnsProp), CommonConf.get().MAX_COLUMNS);
    TFS.createRawTable(new TachyonURI("/table"), CommonConf.get().MAX_COLUMNS);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest2() throws IOException {
    TFS.createRawTable(new TachyonURI("/table"), 0);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest3() throws IOException {
    TFS.createRawTable(new TachyonURI("/table"), -1);
  }

  @Test
  public void deleteFileTest() throws IOException {
    final String path = TestUtils.uniqFile();
    List<ClientWorkerInfo> workers = TFS.getWorkersInfo();
    Assert.assertEquals(1, workers.size());
    Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
    Assert.assertEquals(0, workers.get(0).getUsedBytes());
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;

    // Delete non-existing files.
    Assert.assertTrue(TFS.delete(Integer.MAX_VALUE, false));
    Assert.assertTrue(TFS.delete(Integer.MAX_VALUE, true));
    Assert.assertTrue(TFS.delete(new TachyonURI(path + "/abc"), false));
    Assert.assertTrue(TFS.delete(new TachyonURI(path + "/abc"), true));

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(path + "/file" + k);
      int fileId = TestUtils.createByteFile(TFS, fileURI, WriteType.MUST_CACHE, writeBytes);
      TachyonFile file = TFS.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
      Assert.assertTrue(TFS.exist(fileURI));

      workers = TFS.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
      Assert.assertEquals(writeBytes * (k + 1), workers.get(0).getUsedBytes());
    }

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(path + "/file" + k);
      int fileId = TFS.getFileId(fileURI);
      TFS.delete(fileId, true);
      Assert.assertFalse(TFS.exist(fileURI));

      CommonUtils.sleepMs(null, SLEEP_MS);
      workers = TFS.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
      Assert.assertEquals(writeBytes * (4 - k), workers.get(0).getUsedBytes());
    }
  }

  @Test
  public void getFileStatusTest() throws IOException {
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI("/file");
    int fileId = TestUtils.createByteFile(TFS, uri, WriteType.MUST_CACHE, writeBytes);
    TachyonFile file = TFS.getFile(fileId);
    Assert.assertTrue(file.isInMemory());
    Assert.assertTrue(TFS.exist(uri));
    ClientFileInfo fileInfo = TFS.getFileStatus(fileId, false);
    Assert.assertTrue(fileInfo.getPath().equals("/file"));
  }

  @Test
  public void getFileStatusCacheTest() throws IOException {
    final String path = TestUtils.uniqFile();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(path + "/file");
    int fileId = TestUtils.createByteFile(TFS, uri, WriteType.MUST_CACHE, writeBytes);
    TachyonFile file = TFS.getFile(fileId);
    Assert.assertTrue(file.isInMemory());
    Assert.assertTrue(TFS.exist(uri));
    ClientFileInfo fileInfo = TFS.getFileStatus(fileId, false);
    Assert.assertTrue(fileInfo.getPath().equals(path + "/file"));
    ClientFileInfo fileInfoCached = TFS.getFileStatus(fileId, true);
    ClientFileInfo fileInfoNotCached = TFS.getFileStatus(fileId, false);
    Assert.assertTrue(fileInfo == fileInfoCached);
    Assert.assertFalse(fileInfo == fileInfoNotCached);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal1() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS.get(new TachyonURI("/" + host + ":" + port));
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal2() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS.get(new TachyonURI("/" + host + port));
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal3() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS.get(new TachyonURI("/" + host + ":" + (port - 1)));
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal4() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS.get(new TachyonURI("/" + host + ":" + port + "/ab/c.txt"));
  }

  private void getTestHelper(TachyonFS tfs) throws IOException {
    final String path = TestUtils.uniqFile();
    int fileId = tfs.createFile(new TachyonURI(path));
    Assert.assertNotNull(tfs.getFile(fileId));
  }

  @Test
  public void getTestNormal1() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://" + host + ":" + port));
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal2() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://" + host + ":" + port + "/"));
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal3() throws IOException {
    String host = CLUSTER.getMasterHostname();
    int port = CLUSTER.getMasterPort();
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://" + host + ":" + port + "/ab/c.txt"));
    getTestHelper(tfs);
  }

  @Test
  public void lockBlockTest1() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    fileIds.add(TestUtils.createByteFile(TFS, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = TFS.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void lockBlockTest2() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    final String path = TestUtils.uniqFile();
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      Assert.assertNotNull(tFile.readByteBuffer(0));
    }
    fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = TFS.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void lockBlockTest3() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    final String path = TestUtils.uniqFile();
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      if (k < numOfFiles - 1) {
        Assert.assertNotNull(tFile.readByteBuffer(0));
      }
    }
    fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      if (k != numOfFiles - 1) {
        Assert.assertTrue(tFile.isInMemory());
      } else {
        CommonUtils.sleepMs(null, SLEEP_MS);
        Assert.assertFalse(tFile.isInMemory());
      }
    }
  }

  @Test
  public void lockBlockTest4() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    final String path = TestUtils.uniqFile();
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k <= numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      CommonUtils.sleepMs(null, SLEEP_MS);
      Assert.assertFalse(tFile.isInMemory());
      if (k < numOfFiles) {
        Assert.assertNull(tFile.readByteBuffer(0));
        Assert.assertTrue(tFile.recache());
        Assert.assertNotNull(tFile.readByteBuffer(0));
      } else {
        Assert.assertNull(tFile.readByteBuffer(0));
        Assert.assertFalse(tFile.recache());
        Assert.assertNull(tFile.readByteBuffer(0));
      }
    }
  }

  @Test
  public void mkdirTest() throws IOException {
    final String path = TestUtils.uniqFile();
    for (int k = 0; k < 10; k ++) {
      Assert.assertEquals(true, TFS.mkdir(new TachyonURI(path + "/root/folder" + k)));
      Assert.assertEquals(true, TFS.mkdir(new TachyonURI(path + "/root/folder" + k)));
    }
  }

  @Test
  public void renameFileTest1() throws IOException {
    final String path = TestUtils.uniqFile();
    int fileId = TFS.createFile(new TachyonURI(path + 1));
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(path + k);
      TachyonURI fileB = new TachyonURI(path + (k + 1));
      Assert.assertTrue(TFS.exist(fileA));
      Assert.assertTrue(TFS.rename(fileA, fileB));
      Assert.assertEquals(fileId, TFS.getFileId(fileB));
      Assert.assertFalse(TFS.exist(fileA));
    }
  }

  @Test
  public void renameFileTest2() throws IOException {
    final String path = TestUtils.uniqFile();
    TFS.createFile(new TachyonURI(path));
    Assert.assertTrue(TFS.rename(new TachyonURI(path), new TachyonURI(path)));
  }

  @Test
  public void renameFileTest3() throws IOException {
    final String path = TestUtils.uniqFile();
    TachyonURI file0 = new TachyonURI(path + 0);
    int fileId = TFS.createFile(file0);
    TachyonFile file = TFS.getFile(file0);
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(path + (k - 1));
      TachyonURI fileB = new TachyonURI(path + k);
      Assert.assertTrue(TFS.exist(fileA));
      Assert.assertTrue(file.rename(fileB));
      Assert.assertEquals(fileId, TFS.getFileId(fileB));
      Assert.assertFalse(TFS.exist(fileA));
    }
  }

  @Test
  public void toStringTest() throws IOException {
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://127.0.0.1:19998"));
    Assert.assertEquals(tfs.toString(), "tachyon:///127.0.0.1:19998");
  }

  @Test
  public void unlockBlockTest1() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    final String path = TestUtils.uniqFile();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = TFS.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    final String path = TestUtils.uniqFile();
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = TFS.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void unlockBlockTest3() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    final String path = TestUtils.uniqFile();
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf1 = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf1);
      TachyonByteBuffer tBuf2 = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf2);
      tBuf1.close();
      tBuf2.close();
    }
    fileIds.add(TestUtils.createByteFile(TFS, path + "/file_" + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = TFS.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = TFS.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

}
