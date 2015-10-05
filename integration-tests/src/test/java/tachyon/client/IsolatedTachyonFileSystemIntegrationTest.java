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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterContext;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Do not reuse the LocalTachyonCluster).
 */
public class IsolatedTachyonFileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private TachyonConf mMasterTachyonConf;
  private TachyonConf mWorkerTachyonConf;
  private int mWorkerToMasterHeartbeatIntervalMs;
  private OutStreamOptions mWriteBoth;
  private OutStreamOptions mWriteUnderStorage;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    MasterContext.getConf().set(Constants.USER_FILE_BUFFER_BYTES, Integer.toString(
        USER_QUOTA_UNIT_BYTES));

    mLocalTachyonCluster =
        new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, 100 * Constants.MB);
    mLocalTachyonCluster.start();

    mTfs = mLocalTachyonCluster.getClient();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mWorkerTachyonConf.set(Constants.MAX_COLUMNS, "257");
    mWorkerToMasterHeartbeatIntervalMs =
        mWorkerTachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    mWriteBoth =
        new OutStreamOptions.Builder(mWorkerTachyonConf)
            .setTachyonStorageType(TachyonStorageType.STORE)
            .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
    mWriteUnderStorage =
        new OutStreamOptions.Builder(mWorkerTachyonConf)
            .setTachyonStorageType(TachyonStorageType.NO_STORE)
            .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
  }

  @Test
  public void lockBlockTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonFile> files = new ArrayList<TachyonFile>();
    for (int k = 0; k < numOfFiles; k ++) {
      files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      Assert.assertTrue(mTfs.getInfo(files.get(k)).getInMemoryPercentage() == 100);
    }
    files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth));

    CommonUtils.sleepMs(mWorkerToMasterHeartbeatIntervalMs);

    Assert.assertFalse(mTfs.getInfo(files.get(0)).getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      Assert.assertTrue(mTfs.getInfo(files.get(k)).getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void lockBlockTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    FileInStream is = null;
    ByteBuffer buf = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonFile> files = new ArrayList<TachyonFile>();
    for (int k = 0; k < numOfFiles; k ++) {
      files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.getInStream(files.get(k), TachyonFSTestUtils.toInStreamOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth));

    for (int k = 1; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    FileInfo info = mTfs.getInfo(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void lockBlockTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    FileInStream is = null;
    ByteBuffer buf = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonFile> files = new ArrayList<TachyonFile>();
    for (int k = 0; k < numOfFiles; k ++) {
      files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.getInStream(files.get(k), TachyonFSTestUtils.toInStreamOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      int r = is.read(buf.array());
      if (k < numOfFiles - 1) {
        Assert.assertTrue(r != -1);
      }
      is.close();
    }
    files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth));

    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    FileInfo info = mTfs.getInfo(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    FileInStream is = null;
    ByteBuffer buf = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonFile> files = new ArrayList<TachyonFile>();
    for (int k = 0; k < numOfFiles; k ++) {
      files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      is = mTfs.getInStream(files.get(k), TachyonFSTestUtils.toInStreamOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth));

    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    FileInfo info = mTfs.getInfo(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      FileInfo in = mTfs.getInfo(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    FileInStream is = null;
    ByteBuffer buf = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonFile> files = new ArrayList<TachyonFile>();
    for (int k = 0; k < numOfFiles; k ++) {
      files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.getInStream(files.get(k), TachyonFSTestUtils.toInStreamOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.seek(0);
      buf.clear();
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth));

    for (int k = 1; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    FileInfo info = mTfs.getInfo(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void unlockBlockTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile tFile = null;
    FileInStream is = null;
    ByteBuffer buf1 = null;
    ByteBuffer buf2 = null;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonFile> files = new ArrayList<TachyonFile>();
    for (int k = 0; k < numOfFiles; k ++) {
      files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      FileInfo info = mTfs.getInfo(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.getInStream(files.get(k), TachyonFSTestUtils.toInStreamOptions(mWriteBoth));
      buf1 = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf1.array()) != -1);
      buf2 = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      is.seek(0);
      Assert.assertTrue(is.read(buf2.array()) != -1);
      is.close();
    }
    files.add(TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth));

    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    FileInfo info = mTfs.getInfo(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      FileInfo in = mTfs.getInfo(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  private long getSleepMs() {
    return mWorkerToMasterHeartbeatIntervalMs * 2 + 10;
  }
}
