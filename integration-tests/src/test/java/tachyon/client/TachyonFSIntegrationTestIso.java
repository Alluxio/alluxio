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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

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
  private int mWorkerToMasterHeartbeatIntervalMs;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster =
        new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mWorkerTachyonConf.set(Constants.MAX_COLUMNS, "257");
    mWorkerToMasterHeartbeatIntervalMs =
        mWorkerTachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
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
    String uniqPath = PathUtils.uniqPath();
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

    CommonUtils.sleepMs(mWorkerToMasterHeartbeatIntervalMs);

    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void lockBlockTest2() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    InStream is = null;
    ByteBuffer buf = null;
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
      is = tFile.getInStream(ReadType.CACHE);
      buf = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
    CommonUtils.sleepMs(getSleepMs());
    tFile = mTfs.getFile(fileIds.get(numOfFiles));
    Assert.assertFalse(tFile.isInMemory());
  }

  @Test
  public void lockBlockTest3() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    InStream is = null;
    ByteBuffer buf = null;
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
      is = tFile.getInStream(ReadType.CACHE);
      buf = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      int r = is.read(buf.array());
      if (k < numOfFiles - 1) {
        Assert.assertTrue(r != -1);
      }
      is.close();
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
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    InStream is = null;
    ByteBuffer buf = null;
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
      is = tFile.getInStream(ReadType.NO_CACHE);
      buf = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      if (k < numOfFiles) {
        Assert.assertEquals(-1, is.read(buf.array()));
        Assert.assertTrue(tFile.recache());
        is.seek(0);
        buf.clear();
        Assert.assertTrue(is.read(buf.array()) != -1);
      } else {
        Assert.assertEquals(-1, is.read(buf.array()));
        Assert.assertFalse(tFile.recache());
        is.seek(0);
        buf.clear();
        Assert.assertEquals(-1, is.read(buf.array()));
      }
      is.close();
    }
  }

  @Test
  public void unlockBlockTest1() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    InStream is = null;
    ByteBuffer buf = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<Integer> fileIds = new ArrayList<Integer>();
    for (int k = 0; k < numOfFiles; k ++) {
      fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, WriteType.CACHE_THROUGH,
          fileSize));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      is = tFile.getInStream(ReadType.CACHE);
      buf = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      Assert.assertTrue(tFile.isInMemory());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    fileIds.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles,
        WriteType.CACHE_THROUGH, fileSize));

    CommonUtils.sleepMs(getSleepMs());
    tFile = mTfs.getFile(fileIds.get(0));
    Assert.assertFalse(tFile.isInMemory());
    for (int k = 1; k <= numOfFiles; k ++) {
      tFile = mTfs.getFile(fileIds.get(k));
      Assert.assertTrue(tFile.isInMemory());
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    InStream is = null;
    ByteBuffer buf = null;
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
      is = tFile.getInStream(ReadType.CACHE);
      buf = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.seek(0);
      buf.clear();
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
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
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    InStream is = null;
    ByteBuffer buf1 = null;
    ByteBuffer buf2 = null;
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
      is = tFile.getInStream(ReadType.CACHE);
      buf1 = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      Assert.assertTrue(is.read(buf1.array()) != -1);
      buf2 = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      is.seek(0);
      Assert.assertTrue(is.read(buf2.array()) != -1);
      is.close();
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
    return mWorkerToMasterHeartbeatIntervalMs * 2 + 10;
  }
}
