/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests the "pin" and "unpin" commands.
 */
public class PinCommandTest extends AbstractAlluxioShellTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallyScheduleRule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TTL_CHECK,
      HeartbeatContext.WORKER_BLOCK_SYNC,
      HeartbeatContext.WORKER_PIN_LIST_SYNC);

  /**
   * Tests the "pin" and "unpin" commands. Creates a file and tests if unpinning it , then pinning
   * it and finally unpinning
   */
  @Test
  public void setIsPinnedTest() throws Exception {
    AlluxioURI filePath = new AlluxioURI("/testFile");
    FileSystemTestUtils.createByteFile(mFileSystem, filePath, WriteType.MUST_CACHE, 1);

    // Ensure that the file exists
    Assert.assertTrue(fileExists(filePath));

    // Unpin an unpinned file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath.toString()));
    Assert.assertFalse(mFileSystem.getStatus(filePath).isPinned());

    // Pin the file
    Assert.assertEquals(0, mFsShell.run("pin", filePath.toString()));
    Assert.assertTrue(mFileSystem.getStatus(filePath).isPinned());

    // Unpin the file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath.toString()));
    Assert.assertFalse(mFileSystem.getStatus(filePath).isPinned());
  }

  /**
   * Tests pinned files are not evicted when Alluxio reaches memory limit. This test case creates
   * three files, each file is half the size of the cluster's capacity. The three files are added
   * sequentially to the cluster, the first file is pinned. When the third file is added, the two
   * previous files have already occupied the whole capacity, so one file needs to be evicted to
   * spare space for the third file. Since the first file is pinned, it will not be evicted, so only
   * the second file will be evicted.
   */
  @Test
  public void setPinTest() throws Exception {
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_PIN_LIST_SYNC, 10,
        TimeUnit.SECONDS));

    AlluxioURI filePathA = new AlluxioURI("/testFileA");
    AlluxioURI filePathB = new AlluxioURI("/testFileB");
    AlluxioURI filePathC = new AlluxioURI("/testFileC");
    int fileSize = SIZE_BYTES / 2;

    FileSystemTestUtils.createByteFile(mFileSystem, filePathA, WriteType.MUST_CACHE, fileSize);
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    Assert.assertTrue(fileExists(filePathA));
    Assert.assertEquals(0, mFsShell.run("pin", filePathA.toString()));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_PIN_LIST_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_PIN_LIST_SYNC, 10,
        TimeUnit.SECONDS));

    FileSystemTestUtils.createByteFile(mFileSystem, filePathB, WriteType.MUST_CACHE, fileSize);
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    Assert.assertTrue(fileExists(filePathB));
    Assert.assertEquals(0, mFsShell.run("unpin", filePathB.toString()));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_PIN_LIST_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_PIN_LIST_SYNC, 10,
        TimeUnit.SECONDS));

    FileSystemTestUtils.createByteFile(mFileSystem, filePathC, WriteType.MUST_CACHE, fileSize);
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    Assert.assertTrue(fileExists(filePathC));

    // fileA is in memory because it is pinned, but fileB should have been evicted to hold fileC.
    Assert.assertEquals(100, mFileSystem.getStatus(filePathA).getInMemoryPercentage());
    Assert.assertEquals(0, mFileSystem.getStatus(filePathB).getInMemoryPercentage());
    // fileC should be in memory because fileB is evicted.
    Assert.assertEquals(100, mFileSystem.getStatus(filePathC).getInMemoryPercentage());
  }
}
