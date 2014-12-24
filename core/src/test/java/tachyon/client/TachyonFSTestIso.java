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

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

/**
 * Unit tests on TachyonClient (Do not reuse the LocalTachyonCluster).
 */
public class TachyonFSTestIso {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static final int SLEEP_MS = WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS * 2 + 10;
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
  public void createFileWithUfsFileTest() throws IOException {
    String tempFolder = mLocalTachyonCluster.getTempFolderInUnderFs();
    UnderFileSystem underFs = UnderFileSystem.get(tempFolder);
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
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + numOfFiles, WriteType.CACHE_THROUGH,
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
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      Assert.assertNotNull(tFile.readByteBuffer(0));
    }
    fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + numOfFiles, WriteType.CACHE_THROUGH,
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
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      if (k < numOfFiles - 1) {
        Assert.assertNotNull(tFile.readByteBuffer(0));
      }
    }
    fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + numOfFiles, WriteType.CACHE_THROUGH,
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
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k <= numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
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
  public void unlockBlockTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
      TachyonByteBuffer tBuf = tFile.readByteBuffer(0);
      Assert.assertNotNull(tBuf);
      tBuf.close();
    }
    fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + numOfFiles, WriteType.CACHE_THROUGH,
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
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
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
    fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + numOfFiles, WriteType.CACHE_THROUGH,
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
    String uniqPath = TestUtils.uniqPath();
    TachyonFile tFile = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH, fileSize));
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
    fileIds.add(TestUtils.createByteFile(mTfs, uniqPath + numOfFiles, WriteType.CACHE_THROUGH,
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
