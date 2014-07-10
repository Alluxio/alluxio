/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.table.RawTable;
import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Unit tests on TachyonClient.
 */
public class TachyonFSTest {
  private final int WORKER_CAPACITY_BYTES = 20000;
  private final int USER_QUOTA_UNIT_BYTES = 1000;
  private final int SLEEP_MS = WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS * 2 + 10;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.max.columns");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    System.setProperty("tachyon.max.columns", "257");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }
  
  @Test
  public void getRootTest() throws IOException {
    Assert.assertEquals(1, mTfs.getFileId("/"));
  }

  @Test
  public void createFileTest() throws IOException  {
    int fileId = mTfs.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    fileId = mTfs.createFile("/root/testFile2");
    Assert.assertEquals(4, fileId);
    fileId = mTfs.createFile("/root/testFile3");
    Assert.assertEquals(5, fileId);
  }

  @Test
  public void createFileTest2() throws IOException {
    Assert.assertEquals(3, mTfs.createFile("/root/testFile1"));
    Assert.assertTrue(mTfs.exist("/root/testFile1"));
    Assert.assertEquals(4, mTfs.createFile("/root/testFile2"));
    Assert.assertTrue(mTfs.exist("/root/testFile2"));
    Assert.assertEquals(5, mTfs.createFile("/root/testFile3"));
    Assert.assertTrue(mTfs.exist("/root/testFile3"));
  }

  @Test
  public void createFileWithUfsFileTest() throws IOException {
    String tempFolder = mLocalTachyonCluster.getTempFolderInUnderFs();
    UnderFileSystem underFs = UnderFileSystem.get(tempFolder);
    OutputStream os = underFs.create(tempFolder + "/temp", 100);
    os.close();
    mTfs.createFile("/abc", tempFolder + "/temp");
    Assert.assertTrue(mTfs.exist("/abc"));
    Assert.assertEquals(tempFolder + "/temp", mTfs.getUfsPath(mTfs.getFileId("/abc")));
  }

  @Test(expected = IOException.class)
  public void createFileWithFileAlreadyExistExceptionTest() throws IOException {
    int fileId = mTfs.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    fileId = mTfs.createFile("/root/testFile1");
  }

  @Test(expected = IOException.class)
  public void createFileWithInvalidPathExceptionTest() throws IOException {
    mTfs.createFile("root/testFile1");
  }

  @Test
  public void createRawTableTestEmptyMetadata() throws IOException {
    int fileId = mTfs.createRawTable("/tables/table1", 20);
    RawTable table = mTfs.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

    table = mTfs.getRawTable("/tables/table1");
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
  }

  @Test
  public void createRawTableTestWithMetadata() throws IOException {
    int fileId = mTfs.createRawTable("/tables/table1", 20, TestUtils.getIncreasingByteBuffer(9));
    RawTable table = mTfs.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());

    table = mTfs.getRawTable("/tables/table1");
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());
  }

  @Test(expected = IOException.class)
  public void createRawTableWithFileAlreadyExistExceptionTest() throws IOException {
    mTfs.createRawTable("/table", 20);
    mTfs.createRawTable("/table", 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithInvalidPathExceptionTest1() throws IOException {
    mTfs.createRawTable("tables/table1", 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithInvalidPathExceptionTest2() throws IOException {
    mTfs.createRawTable("/tab les/table1", 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest1() throws IOException {
    String maxColumnsProp = System.getProperty("tachyon.max.columns");

    Assert.assertEquals(Integer.parseInt(maxColumnsProp), CommonConf.get().MAX_COLUMNS);
    mTfs.createRawTable("/table", CommonConf.get().MAX_COLUMNS);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest2() throws IOException {
    mTfs.createRawTable("/table", 0);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest3() throws IOException {
    mTfs.createRawTable("/table", -1);
  }

  @Test
  public void deleteFileTest() throws IOException {
    List<ClientWorkerInfo> workers = mTfs.getWorkersInfo();
    Assert.assertEquals(1, workers.size());
    Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
    Assert.assertEquals(0, workers.get(0).getUsedBytes());
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;

    // Delete non-existing files.
    Assert.assertTrue(mTfs.delete(2, false));
    Assert.assertTrue(mTfs.delete(2, true));
    Assert.assertTrue(mTfs.delete("/abc", false));
    Assert.assertTrue(mTfs.delete("/abc", true));

    for (int k = 0; k < 5; k ++) {
      int fileId = TestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE, writeBytes);
      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
      Assert.assertTrue(mTfs.exist("/file" + k));

      workers = mTfs.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
      Assert.assertEquals(writeBytes * (k + 1), workers.get(0).getUsedBytes());
    }

    for (int k = 0; k < 5; k ++) {
      int fileId = mTfs.getFileId("/file" + k);
      mTfs.delete(fileId, true);
      Assert.assertFalse(mTfs.exist("/file" + k));

      CommonUtils.sleepMs(null, SLEEP_MS);
      workers = mTfs.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
      Assert.assertEquals(writeBytes * (4 - k), workers.get(0).getUsedBytes());
    }
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal1() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS.get("/" + host + ":" + port);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal2() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS.get("/" + host + port);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal3() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS.get("/" + host + ":" + (port - 1));
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal4() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS.get("/" + host + ":" + port + "/ab/c.txt");
  }

  private void getTestHelper(TachyonFS tfs) throws IOException {
    int fileId = mTfs.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    Assert.assertNotNull(mTfs.getFile(fileId));
  }

  @Test
  public void getTestNormal1() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS tfs = TachyonFS.get("tachyon://" + host + ":" + port);
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal2() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS tfs = TachyonFS.get("tachyon://" + host + ":" + port + "/");
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal3() throws IOException {
    String host = mLocalTachyonCluster.getMasterHostname();
    int port = mLocalTachyonCluster.getMasterPort();
    TachyonFS tfs = TachyonFS.get("tachyon://" + host + ":" + port + "/ab/c.txt");
    getTestHelper(tfs);
  }

  @Test
  public void lockBlockTest1() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void lockBlockTest2() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      Assert.assertNotNull(tFile.readByteBuffer());
    }
    fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = mTfs.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void lockBlockTest3() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      if (k < numOfFiles - 1) {
        Assert.assertNotNull(tFile.readByteBuffer());
      }
    }
    fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
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
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k <= numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      CommonUtils.sleepMs(null, SLEEP_MS);
      Assert.assertFalse(tFile.isInMemory());
      if (k < numOfFiles) {
        Assert.assertNull(tFile.readByteBuffer());
        Assert.assertTrue(tFile.recache());
        Assert.assertNotNull(tFile.readByteBuffer());
      } else {
        Assert.assertNull(tFile.readByteBuffer());
        Assert.assertFalse(tFile.recache());
        Assert.assertNull(tFile.readByteBuffer());
      }
    }
  }

  @Test
  public void mkdirTest() throws IOException {
    for (int k = 0; k < 10; k ++) {
      Assert.assertEquals(true, mTfs.mkdir("/root/folder" + k));
      Assert.assertEquals(true, mTfs.mkdir("/root/folder" + k));
    }
  }

  @Test
  public void renameFileTest1() throws IOException {
    int fileId = mTfs.createFile("/root/testFile1");
    for (int k = 1; k < 10; k ++) {
      Assert.assertTrue(mTfs.exist("/root/testFile" + k));
      Assert.assertTrue(mTfs.rename("/root/testFile" + k, "/root/testFile" + (k + 1)));
      Assert.assertEquals(fileId, mTfs.getFileId("/root/testFile" + (k + 1)));
      Assert.assertFalse(mTfs.exist("/root/testFile" + k));
    }
  }

  @Test
  public void renameFileTest2() throws IOException {
    mTfs.createFile("/root/testFile1");
    Assert.assertTrue(mTfs.rename("/root/testFile1", "/root/testFile1"));
  }

  @Test
  public void renameFileTest3() throws IOException {
    int fileId = mTfs.createFile("/root/testFile0");
    TachyonFile file = mTfs.getFile("/root/testFile0");
    for (int k = 1; k < 10; k ++) {
      Assert.assertTrue(mTfs.exist("/root/testFile" + (k - 1)));
      Assert.assertTrue(file.rename("/root/testFile" + k));
      Assert.assertEquals(fileId, mTfs.getFileId("/root/testFile" + k));
      Assert.assertFalse(mTfs.exist("/root/testFile" + (k - 1)));
    }
  }

  @Test
  public void toStringTest() throws IOException {
    String tfsAddress = "tachyon://localhost:19998";
    TachyonFS tfs = TachyonFS.get(tfsAddress);
    Assert.assertEquals(tfs.toString(), "tachyon://localhost/127.0.0.1:19998");
  }

  @Test
  public void unlockBlockTest1() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer();
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer();
      Assert.assertNotNull(tBuf);
      tBuf = tFile.readByteBuffer();
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = mTfs.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void unlockBlockTest3() throws IOException {
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf1 = tFile.readByteBuffer();
      Assert.assertNotNull(tBuf1);
      TachyonByteBuffer tBuf2 = tFile.readByteBuffer();
      Assert.assertNotNull(tBuf2);
      tBuf1.close();
      tBuf2.close();
    }
    fileIds.add(TestUtils.createByteFile(mTfs, "/file_" + numOfFiles, WriteType.CACHE_THROUGH,
        fileSize));

    CommonUtils.sleepMs(null, SLEEP_MS);
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

}
