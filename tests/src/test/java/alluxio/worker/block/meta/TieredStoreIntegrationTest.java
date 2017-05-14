/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.meta;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link alluxio.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest extends BaseIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 1000;

  private FileSystem mFileSystem;
  private SetAttributeOptions mSetPinned;
  private SetAttributeOptions mSetUnpinned;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
          HeartbeatContext.MASTER_TTL_CHECK,
          HeartbeatContext.WORKER_BLOCK_SYNC,
          HeartbeatContext.WORKER_PIN_LIST_SYNC);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, MEM_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, 1000)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(100))
          .setProperty(PropertyKey.WORKER_FILE_BUFFER_SIZE, String.valueOf(100))
          .build();

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mSetPinned = SetAttributeOptions.defaults().setPinned(true);
    mSetUnpinned = SetAttributeOptions.defaults().setPinned(false);
  }

  /**
   * Tests that deletes go through despite failing initially due to concurrent read.
   */
  @Test
  public void deleteWhileRead() throws Exception {
    HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10, TimeUnit.SECONDS);

    AlluxioURI file = new AlluxioURI("/test1");
    FileSystemTestUtils.createByteFile(mFileSystem, file, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    Assert.assertEquals(100, mFileSystem.getStatus(file).getInMemoryPercentage());
    // Open the file
    OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.CACHE);
    FileInStream in = mFileSystem.openFile(file, options);
    Assert.assertEquals(0, in.read());

    // Delete the file
    mFileSystem.delete(file);

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // After the delete, the master should no longer serve the file
    Assert.assertFalse(mFileSystem.exists(file));

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[MEM_CAPACITY_BYTES];
    Assert.assertEquals(MEM_CAPACITY_BYTES - 1, in.read(res, 1, MEM_CAPACITY_BYTES - 1));
    res[0] = 0;
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(MEM_CAPACITY_BYTES, res));
    in.close();

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // After the file is closed, the master's delete should go through and new files can be created
    AlluxioURI newFile = new AlluxioURI("/test2");
    FileSystemTestUtils.createByteFile(mFileSystem, newFile, WriteType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertEquals(100, mFileSystem.getStatus(newFile).getInMemoryPercentage());
  }

  /**
   * Tests that pinning a file prevents it from being evicted.
   */
  @Test
  public void pinFile() throws Exception {
    // Create a file that fills the entire Alluxio store
    AlluxioURI file = new AlluxioURI("/test1");
    FileSystemTestUtils.createByteFile(mFileSystem, file, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // Pin the file
    mFileSystem.setAttribute(file, mSetPinned);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_PIN_LIST_SYNC);

    // Confirm the pin with master
    Assert.assertTrue(mFileSystem.getStatus(file).isPinned());
    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    mThrown.expect(Exception.class);
    FileSystemTestUtils.createByteFile(mFileSystem, "/test2", WriteType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
  }

  /**
   * Tests that pinning a file and then unpinning.
   */
  @Test
  public void unpinFile() throws Exception {
    // Create a file that fills the entire Alluxio store
    AlluxioURI file1 = new AlluxioURI("/test1");
    FileSystemTestUtils
        .createByteFile(mFileSystem, file1, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // Pin the file
    mFileSystem.setAttribute(file1, mSetPinned);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_PIN_LIST_SYNC);
    // Confirm the pin with master
    Assert.assertTrue(mFileSystem.getStatus(file1).isPinned());

    // Unpin the file
    mFileSystem.setAttribute(file1, mSetUnpinned);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_PIN_LIST_SYNC);
    // Confirm the unpin
    Assert.assertFalse(mFileSystem.getStatus(file1).isPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, this
    // should succeed
    AlluxioURI file2 = new AlluxioURI("/test2");
    FileSystemTestUtils
        .createByteFile(mFileSystem, file2, WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // File 2 should be in memory and File 1 should be evicted
    Assert.assertEquals(0, mFileSystem.getStatus(file1).getInMemoryPercentage());
    Assert.assertEquals(100, mFileSystem.getStatus(file2).getInMemoryPercentage());
  }

  /**
   * Tests the promotion of a file.
   */
  @Test
  public void promoteBlock() throws Exception {
    AlluxioURI uri1 = new AlluxioURI("/file1");
    AlluxioURI uri2 = new AlluxioURI("/file2");
    AlluxioURI uri3 = new AlluxioURI("/file3");
    FileSystemTestUtils.createByteFile(mFileSystem, uri1, WriteType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 6);
    FileSystemTestUtils.createByteFile(mFileSystem, uri2, WriteType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 2);
    FileSystemTestUtils.createByteFile(mFileSystem, uri3, WriteType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 2);

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    AlluxioURI toPromote;
    int toPromoteLen;
    URIStatus file1Info = mFileSystem.getStatus(uri1);
    URIStatus file2Info = mFileSystem.getStatus(uri2);
    URIStatus file3Info = mFileSystem.getStatus(uri3);

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
        mFileSystem.openFile(toPromote,
            OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE));
    byte[] buf = new byte[toPromoteLen];
    int len = is.read(buf);
    is.close();

    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10, TimeUnit.SECONDS);

    Assert.assertEquals(toPromoteLen, len);
    Assert.assertEquals(100, mFileSystem.getStatus(toPromote).getInMemoryPercentage());
  }
}
