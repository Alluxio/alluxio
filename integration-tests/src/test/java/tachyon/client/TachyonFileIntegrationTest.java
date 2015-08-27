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
import java.io.RandomAccessFile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileBlockInfo;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for tachyon.client.TachyonFile.
 */
public class TachyonFileIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final int MAX_FILES = WORKER_CAPACITY_BYTES / USER_QUOTA_UNIT_BYTES;
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  private TachyonConf mWorkerTachyonConf;

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
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();

    TachyonConf masterConf = mLocalTachyonCluster.getMasterTachyonConf();
    int userCapacityBytes = WORKER_CAPACITY_BYTES / 4;
    masterConf.set(Constants.USER_FILE_BUFFER_BYTES, Integer.toString(userCapacityBytes));
  }

  /**
   * Basic isInMemory Test.
   *
   * @throws IOException
   */
  @Test
  public void isInMemoryTest() throws IOException {
    int fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/file1", WriteType.MUST_CACHE,
            USER_QUOTA_UNIT_BYTES);
    TachyonFile file = mTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/file2", WriteType.CACHE_THROUGH,
            USER_QUOTA_UNIT_BYTES);
    file = mTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/file3", WriteType.THROUGH, USER_QUOTA_UNIT_BYTES);
    file = mTfs.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertTrue(file.recache());
    Assert.assertTrue(file.isInMemory());

    fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/file4", WriteType.THROUGH,
            WORKER_CAPACITY_BYTES + 1);
    file = mTfs.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertFalse(file.recache());
    Assert.assertFalse(file.isInMemory());

    fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/file5", WriteType.THROUGH, WORKER_CAPACITY_BYTES);
    file = mTfs.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertTrue(file.recache());
    Assert.assertTrue(file.isInMemory());
  }

  /**
   * Test LRU Cache Eviction.
   *
   * @throws IOException
   */
  // TODO: Reenable this when LRU is enabled
  /*
  @Test
  public void isInMemoryTest2() throws IOException {
    for (int k = 0; k < MAX_FILES; k ++) {
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE,
              USER_QUOTA_UNIT_BYTES);
      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils
        .sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 2 + 10);

    for (int k = 0; k < MAX_FILES; k ++) {
      TachyonFile file = mTfs.getFile(new TachyonURI("/file" + k));
      Assert.assertTrue(file.isInMemory());
    }

    for (int k = MAX_FILES; k < MAX_FILES + 1; k ++) {
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE,
              USER_QUOTA_UNIT_BYTES);
      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils
        .sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 2 + 10);

    TachyonFile file = mTfs.getFile(new TachyonURI("/file" + 0));
    Assert.assertFalse(file.isInMemory());

    for (int k = 1; k < MAX_FILES + 1; k ++) {
      file = mTfs.getFile(new TachyonURI("/file" + k));
      Assert.assertTrue(file.isInMemory());
    }
  }
  */

  /**
   * Test LRU Cache Eviction + PIN.
   *
   * @throws IOException
   */
  // TODO: Reenable this when LRU is enabled
  /*
  @Test
  public void isInMemoryTest3() throws IOException {
    TachyonURI pin = new TachyonURI("/pin");
    mTfs.mkdir(pin);
    mTfs.pinFile(mTfs.getFileId(pin));

    int fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/pin/file", WriteType.MUST_CACHE,
            USER_QUOTA_UNIT_BYTES);
    TachyonFile file = mTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    for (int k = 0; k < MAX_FILES; k ++) {
      fileId =
          TachyonFSTestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE,
              USER_QUOTA_UNIT_BYTES);
      file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils
        .sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 2 + 10);

    file = mTfs.getFile(new TachyonURI("/pin/file"));
    Assert.assertTrue(file.isInMemory());
    file = mTfs.getFile(new TachyonURI("/file0"));
    Assert.assertFalse(file.isInMemory());
    for (int k = 1; k < MAX_FILES; k ++) {
      file = mTfs.getFile(new TachyonURI("/file" + k));
      Assert.assertTrue(file.isInMemory());
    }
  }
  */
  /**
   * Test <code>String getLocalFilename(long blockId) </code>.
   */
  @Test
  public void readLocalTest() throws IOException {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_"
              + WriteType.MUST_CACHE, WriteType.MUST_CACHE, k);

      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertEquals(1, file.getNumberOfBlocks());
      String localFname = file.getLocalFilename(0);
      Assert.assertNotNull("Block not found on local ramdisk", localFname);
      RandomAccessFile lfile = new RandomAccessFile(localFname, "r");
      byte[] buf = new byte[k];
      lfile.read(buf, 0, k);

      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, buf));

      lfile.close();
    }
  }

  @Test
  public void readRemoteTest() throws IOException {
    int fileId =
        TachyonFSTestUtils.createByteFile(mTfs, "/root/testFile", WriteType.MUST_CACHE,
            USER_QUOTA_UNIT_BYTES);

    TachyonFile file = mTfs.getFile(fileId);
    FileBlockInfo blockInfo = file.getClientBlockInfo(0);
    TachyonByteBuffer buf = file.readRemoteByteBuffer(blockInfo);
    Assert.assertEquals(USER_QUOTA_UNIT_BYTES, buf.mData.limit());
    buf.close();
  }

  @Test
  public void writeEmptyFileTest() throws IOException {
    TachyonURI uri = new TachyonURI("/emptyFile");
    Assert.assertEquals(2, mTfs.createFile(uri));
    Assert.assertTrue(mTfs.exist(uri));
    TachyonFile file = mTfs.getFile(uri);
    Assert.assertEquals(0, file.length());
    OutStream os = file.getOutStream(WriteType.CACHE_THROUGH);
    os.close();
    Assert.assertEquals(0, file.length());
  }
}
