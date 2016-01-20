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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Do not reuse the LocalTachyonCluster).
 */
public class IsolatedFileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource = new LocalTachyonClusterResource(
      WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, 100 * Constants.MB,
      Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mTfs = null;
  private int mWorkerToMasterHeartbeatIntervalMs;
  private CreateFileOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mTfs = mLocalTachyonClusterResource.get().getClient();

    TachyonConf workerTachyonConf = mLocalTachyonClusterResource.get().getWorkerTachyonConf();
    mWorkerToMasterHeartbeatIntervalMs =
        workerTachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(workerTachyonConf);
  }

  @Test
  public void lockBlockTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonURI> files = new ArrayList<TachyonURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth);
      files.add(new TachyonURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      Assert.assertTrue(mTfs.getStatus(files.get(k)).getInMemoryPercentage() == 100);
    }
    TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new TachyonURI(uniqPath + numOfFiles));

    CommonUtils.sleepMs(mWorkerToMasterHeartbeatIntervalMs);

    Assert.assertFalse(mTfs.getStatus(files.get(0)).getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      Assert.assertTrue(mTfs.getStatus(files.get(k)).getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void lockBlockTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonURI> files = new ArrayList<TachyonURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth);
      files.add(new TachyonURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.openFile(files.get(k), TachyonFSTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new TachyonURI(uniqPath + numOfFiles));

    for (int k = 1; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mTfs.getStatus(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void lockBlockTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonURI> files = new ArrayList<TachyonURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth);
      files.add(new TachyonURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.openFile(files.get(k), TachyonFSTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      int r = is.read(buf.array());
      if (k < numOfFiles - 1) {
        Assert.assertTrue(r != -1);
      }
      is.close();
    }
    TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new TachyonURI(uniqPath + numOfFiles));
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mTfs.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonURI> files = new ArrayList<TachyonURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth);
      files.add(new TachyonURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      is = mTfs.openFile(files.get(k), TachyonFSTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new TachyonURI(uniqPath + numOfFiles));
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mTfs.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      URIStatus in = mTfs.getStatus(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonURI> files = new ArrayList<TachyonURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth);
      files.add(new TachyonURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.openFile(files.get(k), TachyonFSTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.seek(0);
      buf.clear();
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new TachyonURI(uniqPath + numOfFiles));
    for (int k = 1; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mTfs.getStatus(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void unlockBlockTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf1;
    ByteBuffer buf2;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<TachyonURI> files = new ArrayList<TachyonURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      TachyonFSTestUtils.createByteFile(mTfs, uniqPath + k, fileSize, mWriteBoth);
      files.add(new TachyonURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mTfs.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mTfs.openFile(files.get(k), TachyonFSTestUtils.toOpenFileOptions(mWriteBoth));
      buf1 = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf1.array()) != -1);
      buf2 = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      is.seek(0);
      Assert.assertTrue(is.read(buf2.array()) != -1);
      is.close();
    }
    TachyonFSTestUtils.createByteFile(mTfs, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new TachyonURI(uniqPath + numOfFiles));
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mTfs.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      URIStatus in = mTfs.getStatus(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  private long getSleepMs() {
    return mWorkerToMasterHeartbeatIntervalMs * 2 + 10;
  }
}
