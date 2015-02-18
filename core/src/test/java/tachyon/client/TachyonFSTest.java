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
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.table.RawTable;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Unit tests on TachyonClient (Reuse the LocalTachyonCluster).
 */
public class TachyonFSTest {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static String sHost = null;
  private static int sPort = -1;
  private static TachyonFS sTfs = null;
  private TachyonConf mMasterTachyonConf;
  private TachyonConf mWorkerTachyonConf;


  @Before
  public final void before() throws IOException {
    mMasterTachyonConf = sLocalTachyonCluster.getMasterTachyonConf();
    mMasterTachyonConf.set(Constants.MAX_COLUMNS, "257");
    mWorkerTachyonConf = sLocalTachyonCluster.getWorkerTachyonConf();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.max.columns");
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    sLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES,
        Constants.GB);
    sLocalTachyonCluster.start();
    sTfs = sLocalTachyonCluster.getClient();
    sHost = sLocalTachyonCluster.getMasterHostname();
    sPort = sLocalTachyonCluster.getMasterPort();
  }

  @Test
  public void getRootTest() throws IOException {
    Assert.assertEquals(1, sTfs.getFileId(new TachyonURI(TachyonURI.SEPARATOR)));
  }

  @Test
  public void createFileTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = 1; k < 5; k ++) {
      TachyonURI uri = new TachyonURI(uniqPath + k);
      int fileId = sTfs.createFile(uri);
      Assert.assertTrue(sTfs.exist(uri));
      Assert.assertEquals(fileId, sTfs.getFileId(uri));
    }
  }

  @Test(expected = IOException.class)
  public void createFileWithFileAlreadyExistExceptionTest() throws IOException {
    TachyonURI uri = new TachyonURI(TestUtils.uniqPath());
    int fileId = sTfs.createFile(uri);
    Assert.assertEquals(fileId, sTfs.getFileId(uri));
    fileId = sTfs.createFile(uri);
  }

  @Test(expected = IOException.class)
  public void createFileWithInvalidPathExceptionTest() throws IOException {
    sTfs.createFile(new TachyonURI("root/testFile1"));
  }

  @Test
  public void createRawTableTestEmptyMetadata() throws IOException {
    String uniqPath = TestUtils.uniqPath() + "/table1";
    int fileId = sTfs.createRawTable(new TachyonURI(uniqPath), 20);
    RawTable table = sTfs.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals(uniqPath, table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

    table = sTfs.getRawTable(new TachyonURI(uniqPath));
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals(uniqPath, table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
  }

  @Test
  public void createRawTableTestWithMetadata() throws IOException {
    String uniqPath = TestUtils.uniqPath() + "/table1";
    TachyonURI uri = new TachyonURI(uniqPath);
    int fileId = sTfs.createRawTable(uri, 20, TestUtils.getIncreasingByteBuffer(9));
    RawTable table = sTfs.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals(uniqPath, table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());

    table = sTfs.getRawTable(uri);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals(uniqPath, table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());
  }

  @Test(expected = IOException.class)
  public void createRawTableWithFileAlreadyExistExceptionTest() throws IOException {
    String uniqPath = TestUtils.uniqPath() + "/table";
    TachyonURI uri = new TachyonURI(uniqPath);
    sTfs.createRawTable(uri, 20);
    sTfs.createRawTable(uri, 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithInvalidPathExceptionTest1() throws IOException {
    sTfs.createRawTable(new TachyonURI("tables/table1"), 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithInvalidPathExceptionTest2() throws IOException {
    sTfs.createRawTable(new TachyonURI("/tab les/table1"), 20);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest1() throws IOException {
    int maxColumns = mMasterTachyonConf.getInt(Constants.MAX_COLUMNS, 257);
    sTfs.createRawTable(new TachyonURI("/table"), maxColumns);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest2() throws IOException {
    sTfs.createRawTable(new TachyonURI(TestUtils.uniqPath()), 0);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest3() throws IOException {
    sTfs.createRawTable(new TachyonURI(TestUtils.uniqPath()), -1);
  }

  @Test(expected = IOException.class)
  public void createRawTableWithTableColumnExceptionTest4() throws IOException {
    int maxColumns = mMasterTachyonConf.getInt(Constants.MAX_COLUMNS, 1000);
    sTfs.createRawTable(new TachyonURI(TestUtils.uniqPath()), maxColumns);
  }

  @Test
  public void deleteFileTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    List<ClientWorkerInfo> workers = sTfs.getWorkersInfo();
    Assert.assertEquals(1, workers.size());
    Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;

    // Delete non-existing files.
    Assert.assertTrue(sTfs.delete(new TachyonURI(uniqPath), false));
    Assert.assertTrue(sTfs.delete(new TachyonURI(uniqPath), true));

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      int fileId = TestUtils.createByteFile(sTfs, fileURI, WriteType.MUST_CACHE, writeBytes);
      TachyonFile file = sTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
      Assert.assertTrue(sTfs.exist(fileURI));

      workers = sTfs.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
    }

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      int fileId = sTfs.getFileId(fileURI);
      sTfs.delete(fileId, true);
      Assert.assertFalse(sTfs.exist(fileURI));
      int timeOutMs = TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 2 + 10;
      CommonUtils.sleepMs(null, timeOutMs);
      workers = sTfs.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
    }
  }

  @Test
  public void getFileStatusTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(uniqPath);
    int fileId = TestUtils.createByteFile(sTfs, uri, WriteType.MUST_CACHE, writeBytes);
    TachyonFile file = sTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());
    Assert.assertTrue(sTfs.exist(uri));
    ClientFileInfo fileInfo = sTfs.getFileStatus(fileId, false);
    Assert.assertTrue(fileInfo.getPath().equals(uniqPath));
  }

  @Test
  public void getFileStatusCacheTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(uniqPath);
    int fileId = TestUtils.createByteFile(sTfs, uri, WriteType.MUST_CACHE, writeBytes);
    TachyonFile file = sTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());
    Assert.assertTrue(sTfs.exist(uri));
    ClientFileInfo fileInfo = sTfs.getFileStatus(fileId, false);
    Assert.assertTrue(fileInfo.getPath().equals(uniqPath));
    ClientFileInfo fileInfoCached = sTfs.getFileStatus(fileId, true);
    ClientFileInfo fileInfoNotCached = sTfs.getFileStatus(fileId, false);
    Assert.assertTrue(fileInfo == fileInfoCached);
    Assert.assertFalse(fileInfo == fileInfoNotCached);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal1() throws IOException {
    TachyonFS.get(new TachyonURI("/" + sHost + ":" + sPort), mMasterTachyonConf);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal2() throws IOException {
    TachyonFS.get(new TachyonURI("/" + sHost + sPort), mMasterTachyonConf);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal3() throws IOException {
    TachyonFS.get(new TachyonURI("/" + sHost + ":" + (sPort - 1)), mMasterTachyonConf);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal4() throws IOException {
    TachyonFS.get(new TachyonURI("/" + sHost + ":" + sPort + "/ab/c.txt"), mMasterTachyonConf);
  }

  @Test(expected = IOException.class)
  public void getTestAbnormal5() throws IOException {
    // API user may have this typo: tacyon
    TachyonFS.get(new TachyonURI("tacyon://" + sHost + ":" + sPort), mMasterTachyonConf);
  }

  private void getTestHelper(TachyonFS tfs) throws IOException {
    TachyonURI uri = new TachyonURI(TestUtils.uniqPath());
    int fileId = tfs.createFile(uri);
    Assert.assertEquals(fileId, tfs.getFileId(uri));
    Assert.assertNotNull(tfs.getFile(fileId));
  }

  @Test
  public void getTestNormal1() throws IOException {
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://" + sHost + ":" + sPort),
        mMasterTachyonConf);
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal2() throws IOException {
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://" + sHost + ":" + sPort + "/"),
        mMasterTachyonConf);
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal3() throws IOException {
    TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://" + sHost + ":" + sPort + "/ab/c.txt"),
        mMasterTachyonConf);
    getTestHelper(tfs);
  }

  @Test
  public void getTestNormal4() throws IOException {
    TachyonConf copyConf = new TachyonConf(mMasterTachyonConf);
    copyConf.set(Constants.MASTER_HOSTNAME, sHost);
    copyConf.set(Constants.MASTER_PORT, Integer.toString(sPort));
    copyConf.set(Constants.USE_ZOOKEEPER, "false");

    TachyonFS tfs = TachyonFS.get(copyConf);
    getTestHelper(tfs);
  }

  @Test
  public void mkdirTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = 0; k < 10; k ++) {
      Assert.assertTrue(sTfs.mkdir(new TachyonURI(uniqPath + k)));
      Assert.assertTrue(sTfs.mkdir(new TachyonURI(uniqPath + k)));
    }
  }

  @Test
  public void renameFileTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = sTfs.createFile(new TachyonURI(uniqPath + 1));
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(uniqPath + k);
      TachyonURI fileB = new TachyonURI(uniqPath + (k + 1));
      Assert.assertTrue(sTfs.exist(fileA));
      Assert.assertTrue(sTfs.rename(fileA, fileB));
      Assert.assertEquals(fileId, sTfs.getFileId(fileB));
      Assert.assertFalse(sTfs.exist(fileA));
    }
  }

  @Test
  public void renameFileTest2() throws IOException {
    TachyonURI uniqUri = new TachyonURI(TestUtils.uniqPath());
    int fileId = sTfs.createFile(uniqUri);
    Assert.assertTrue(sTfs.rename(uniqUri, uniqUri));
    Assert.assertEquals(fileId, sTfs.getFileId(uniqUri));
  }

  @Test
  public void renameFileTest3() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonURI file0 = new TachyonURI(uniqPath + 0);
    int fileId = sTfs.createFile(file0);
    TachyonFile file = sTfs.getFile(file0);
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(uniqPath + (k - 1));
      TachyonURI fileB = new TachyonURI(uniqPath + k);
      Assert.assertTrue(sTfs.exist(fileA));
      Assert.assertTrue(file.rename(fileB));
      Assert.assertEquals(fileId, sTfs.getFileId(fileB));
      Assert.assertFalse(sTfs.exist(fileA));
    }
  }

  @Test
  public void toStringTest() throws IOException {
    TachyonFS tfs = null;
    String[] originUrls =
        new String[] {"tachyon://127.0.0.1:19998", "tachyon-ft://127.0.0.1:19998",};
    String[] resultUrls =
        new String[] {"tachyon://localhost/127.0.0.1:19998", "tachyon-ft://localhost/127.0.0.1:19998",};
    for (int i = 0, n = originUrls.length; i < n; i ++) {
      String originUrl = originUrls[i];
      String resultUrl = resultUrls[i];
      tfs = TachyonFS.get(new TachyonURI(originUrl), mMasterTachyonConf);
      Assert.assertEquals(resultUrl, tfs.toString());
      tfs = TachyonFS.get(new TachyonURI(originUrl + "/a/b"), mMasterTachyonConf);
      Assert.assertEquals(resultUrl, tfs.toString());
    }

    TachyonConf copyConf = new TachyonConf(mMasterTachyonConf);
    copyConf.set(Constants.MASTER_HOSTNAME, "localhost");
    copyConf.set(Constants.MASTER_PORT, "19998");

    copyConf.set(Constants.USE_ZOOKEEPER, "false");
    tfs = TachyonFS.get(copyConf);
    Assert.assertEquals("tachyon://localhost/127.0.0.1:19998", tfs.toString());

    copyConf.set(Constants.USE_ZOOKEEPER, "true");
    tfs = TachyonFS.get(copyConf);
    Assert.assertEquals("tachyon-ft://localhost/127.0.0.1:19998", tfs.toString());
  }
}
