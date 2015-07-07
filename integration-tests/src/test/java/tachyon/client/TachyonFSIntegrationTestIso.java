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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * Integration tests on TachyonClient (Do not reuse the LocalTachyonCluster).
 */
public class TachyonFSIntegrationTestIso {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private TachyonConf mMasterTachyonConf;
  private TachyonConf mWorkerTachyonConf;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster =
        new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mWorkerTachyonConf.set(Constants.MAX_COLUMNS, "257");
  }

  @Test
  public void createFileWithUfsFileTest() throws IOException {
    String tempFolder = mLocalTachyonCluster.getTempFolderInUnderFs();
    UnderFileSystem underFs = UnderFileSystem.get(tempFolder, mMasterTachyonConf);
    OutputStream os = underFs.create(tempFolder + "/temp", 100);
    os.close();
    TachyonURI uri = new TachyonURI("/abc");
    mTfs.createFile(uri, new TachyonURI(tempFolder + "/temp"));
    Assert.assertTrue(mTfs.exist(uri));
    Assert.assertEquals(tempFolder + "/temp", mTfs.getFile(uri).getUfsPath());
  }

  @Test
  public void lockBlockTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    CommonUtils.sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf));
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void lockBlockTest2() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      Assert.assertNotNull(tFile.readByteBuffer(0));
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(null, getSleepMs());
    tFile = mTfs.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void lockBlockTest3() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      if (k < numOfFiles - 1) {
        Assert.assertNotNull(tFile.readByteBuffer(0));
      }
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      if (k != numOfFiles - 1) {
        Assert.assertTrue(tFile.isInMemory());
      } else {
        CommonUtils.sleepMs(null, getSleepMs());
        Assert.assertFalse(tFile.isInMemory());
      }
    }
  }

  @Test
  public void lockBlockTest4() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k <= numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      CommonUtils.sleepMs(null, getSleepMs());
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
  public void unlockBlockTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    CommonUtils.sleepMs(null, getSleepMs());
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(null, getSleepMs());
    tFile = mTfs.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void unlockBlockTest3() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf1 = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf1);
      TachyonByteBuffer tBuf2 = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf2);
      tBuf1.close();
      tBuf2.close();
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    CommonUtils.sleepMs(null, getSleepMs());
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  private long getSleepMs() {
    return (TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 2 + 10);
  }
}
