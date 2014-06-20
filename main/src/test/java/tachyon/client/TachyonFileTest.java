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
import java.io.RandomAccessFile;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;

/**
 * Unit tests for tachyon.client.TachyonFile.
 */
public class TachyonFileTest {
  private final int WORKER_CAPACITY_BYTES = 1000;
  private final int USER_QUOTA_UNIT_BYTES = 100;
  private final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS =
      WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS;
  private final int MAX_FILES = WORKER_CAPACITY_BYTES / USER_QUOTA_UNIT_BYTES;

  private final int MIN_LEN = 0;
  private final int MAX_LEN = 255;
  private final int DELTA = 33;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  /**
   * Basic isInMemory Test.
   * 
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void isInMemoryTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId =
        TestUtils.createByteFile(mTfs, "/file1", WriteType.MUST_CACHE, USER_QUOTA_UNIT_BYTES);
    TachyonFile file = mTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    fileId =
        TestUtils.createByteFile(mTfs, "/file2", WriteType.CACHE_THROUGH, USER_QUOTA_UNIT_BYTES);
    file = mTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    fileId = TestUtils.createByteFile(mTfs, "/file3", WriteType.THROUGH, USER_QUOTA_UNIT_BYTES);
    file = mTfs.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertTrue(file.recache());
    Assert.assertTrue(file.isInMemory());

    fileId =
        TestUtils.createByteFile(mTfs, "/file4", WriteType.THROUGH, WORKER_CAPACITY_BYTES + 1);
    file = mTfs.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertFalse(file.recache());
    Assert.assertFalse(file.isInMemory());

    fileId = TestUtils.createByteFile(mTfs, "/file5", WriteType.THROUGH, WORKER_CAPACITY_BYTES);
    file = mTfs.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertTrue(file.recache());
    Assert.assertTrue(file.isInMemory());
  }

  /**
   * Test LRU Cache Eviction.
   * 
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void isInMemoryTest2() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    for (int k = 0; k < MAX_FILES; k ++) {
      int fileId =
          TestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE, USER_QUOTA_UNIT_BYTES);
      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    for (int k = 0; k < MAX_FILES; k ++) {
      TachyonFile file = mTfs.getFile("/file" + k);
      Assert.assertTrue(file.isInMemory());
    }

    for (int k = MAX_FILES; k < MAX_FILES + 1; k ++) {
      int fileId =
          TestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE, USER_QUOTA_UNIT_BYTES);
      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    TachyonFile file = mTfs.getFile("/file" + 0);
    Assert.assertFalse(file.isInMemory());

    for (int k = 1; k < MAX_FILES + 1; k ++) {
      file = mTfs.getFile("/file" + k);
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test LRU Cache Eviction + PIN.
   * 
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void isInMemoryTest3() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    mTfs.mkdir("/pin");
    mTfs.pinFile(mTfs.getFileId("/pin"));

    int fileId =
        TestUtils.createByteFile(mTfs, "/pin/file", WriteType.MUST_CACHE, USER_QUOTA_UNIT_BYTES);
    TachyonFile file = mTfs.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    for (int k = 0; k < MAX_FILES; k ++) {
      fileId =
          TestUtils.createByteFile(mTfs, "/file" + k, WriteType.MUST_CACHE, USER_QUOTA_UNIT_BYTES);
      file = mTfs.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);

    file = mTfs.getFile("/pin/file");
    Assert.assertTrue(file.isInMemory());
    file = mTfs.getFile("/file0");
    Assert.assertFalse(file.isInMemory());
    for (int k = 1; k < MAX_FILES; k ++) {
      file = mTfs.getFile("/file" + k);
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>String getLocalFilename(long blockId) </code>.
   */
  @Test
  public void readLocalTest() throws IOException {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      int fileId =
          TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + WriteType.MUST_CACHE,
              WriteType.MUST_CACHE, k);

      TachyonFile file = mTfs.getFile(fileId);
      Assert.assertEquals(1, file.getNumberOfBlocks());
      long bid = mTfs.getBlockIdBasedOnOffset(file.FID, 0);
      String localFname = mTfs.getLocalFilename(bid);
      Assert.assertNotNull("Block not found on local ramdisk", localFname);
      RandomAccessFile lfile = new RandomAccessFile(localFname, "r");
      byte[] buf = new byte[k];
      lfile.read(buf, 0, k);

      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, buf));

      lfile.close();
    }
  }

  @Test
  public void writeEmptyFileTest() throws IOException {
    Assert.assertEquals(2, mTfs.createFile("/emptyFile"));
    Assert.assertTrue(mTfs.exist("/emptyFile"));
    TachyonFile file = mTfs.getFile("/emptyFile");
    Assert.assertEquals(0, file.length());
    OutStream os = file.getOutStream(WriteType.CACHE_THROUGH);
    os.close();
    Assert.assertEquals(0, file.length());
  }
}
