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

package tachyon.worker.block.meta;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for {@link tachyon.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private TachyonConf mWorkerConf;
  private FileSystem mTFS;
  private int mWorkerToMasterHeartbeatIntervalMs;
  private SetAttributeOptions mSetPinned;
  private SetAttributeOptions mSetUnpinned;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100));

  @Before
  public final void before() throws Exception {
    mTFS = mLocalTachyonClusterResource.get().getClient();
    mWorkerConf = mLocalTachyonClusterResource.get().getWorkerTachyonConf();
    mWorkerToMasterHeartbeatIntervalMs =
        mWorkerConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);
    mSetPinned = SetAttributeOptions.defaults().setPinned(true);
    mSetUnpinned = SetAttributeOptions.defaults().setPinned(false);
  }

  /**
   * Tests that deletes go through despite failing initially due to concurrent read.
   *
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  @Test
  public void deleteWhileReadTest() throws IOException, TachyonException {
    TachyonURI file = new TachyonURI("/test1");
    TachyonFSTestUtils.createByteFile(mTFS, file, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    Assert.assertTrue(mTFS.getStatus(file).getInMemoryPercentage() == 100);
    // Open the file
    OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.CACHE);
    FileInStream in = mTFS.openFile(file, options);
    Assert.assertEquals(0, in.read());

    // Delete the file
    mTFS.delete(file);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the delete, the master should no longer serve the file
    Assert.assertFalse(mTFS.exists(file));

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[MEM_CAPACITY_BYTES];
    Assert.assertEquals(MEM_CAPACITY_BYTES - 1, in.read(res, 1, MEM_CAPACITY_BYTES - 1));
    res[0] = 0;
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(MEM_CAPACITY_BYTES, res));
    in.close();

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the file is closed, the master's delete should go through and new files can be created
    TachyonURI newFile = new TachyonURI("/test2");
    TachyonFSTestUtils.createByteFile(mTFS, newFile, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertTrue(mTFS.getStatus(newFile).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that pinning a file prevents it from being evicted.
   *
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  @Test
  public void pinFileTest() throws IOException, TachyonException {
    // Create a file that fills the entire Tachyon store
    TachyonURI file = new TachyonURI("/test1");
    TachyonFSTestUtils.createByteFile(mTFS, file, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Pin the file
    mTFS.setAttribute(file, mSetPinned);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the pin with master
    Assert.assertTrue(mTFS.getStatus(file).isPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    mThrown.expect(IOException.class);
    TachyonFSTestUtils.createByteFile(mTFS, "/test2", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
  }

  /**
   * Tests that pinning a file and then unpinning.
   *
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  @Test
  public void unpinFileTest() throws IOException, TachyonException {
    // Create a file that fills the entire Tachyon store
    TachyonURI file1 = new TachyonURI("/test1");
    TachyonFSTestUtils.createByteFile(mTFS, file1, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Pin the file
    mTFS.setAttribute(file1, mSetPinned);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the pin with master
    Assert.assertTrue(mTFS.getStatus(file1).isPinned());

    // Unpin the file
    mTFS.setAttribute(file1, mSetUnpinned);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the unpin
    Assert.assertFalse(mTFS.getStatus(file1).isPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, this
    // should succeed
    TachyonURI file2 = new TachyonURI("/test2");
    TachyonFSTestUtils.createByteFile(mTFS, file2, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // File 2 should be in memory and File 1 should be evicted
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertFalse(mTFS.getStatus(file1).getInMemoryPercentage() == 100);
    Assert.assertTrue(mTFS.getStatus(file2).getInMemoryPercentage() == 100);
  }

  /**
   * Tests the promotion of a file.
   *
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  @Test
  public void promoteBlock() throws IOException, TachyonException {
    TachyonURI uri1 = new TachyonURI("/file1");
    TachyonURI uri2 = new TachyonURI("/file2");
    TachyonURI uri3 = new TachyonURI("/file3");
    TachyonFSTestUtils.createByteFile(mTFS, uri1, WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES / 6);
    TachyonFSTestUtils.createByteFile(mTFS, uri2, WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES / 2);
    TachyonFSTestUtils.createByteFile(mTFS, uri3, WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES / 2);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    TachyonURI toPromote = null;
    int toPromoteLen = 0;
    URIStatus file1Info = mTFS.getStatus(uri1);
    URIStatus file2Info = mTFS.getStatus(uri2);
    URIStatus file3Info = mTFS.getStatus(uri3);

    // We know some file will not be in memory, but not which one since we do not want to make
    // any assumptions on the eviction policy
    if (file1Info.getInMemoryPercentage() < 100) {
      toPromote = uri1;
      toPromoteLen = (int) file1Info.getLength();
      Assert.assertEquals(100, file2Info.getInMemoryPercentage());
      Assert.assertEquals(100, file3Info.getInMemoryPercentage());
    } else if (file2Info.getInMemoryPercentage() < 100) {
      toPromote = uri2;
      toPromoteLen = (int) file2Info.getLength();
      Assert.assertEquals(100, file1Info.getInMemoryPercentage());
      Assert.assertEquals(100, file3Info.getInMemoryPercentage());
    } else {
      toPromote = uri3;
      toPromoteLen = (int) file3Info.getLength();
      Assert.assertEquals(100, file1Info.getInMemoryPercentage());
      Assert.assertEquals(100, file2Info.getInMemoryPercentage());
    }

    FileInStream is =
        mTFS.openFile(toPromote, OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE));
    byte[] buf = new byte[toPromoteLen];
    int len = is.read(buf);
    is.close();

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    Assert.assertEquals(toPromoteLen, len);
    Assert.assertEquals(100, mTFS.getStatus(toPromote).getInMemoryPercentage());
  }
}
