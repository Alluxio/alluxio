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
import alluxio.exception.AlluxioException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for free command.
 */
public class FreeCommandTest extends AbstractAlluxioShellTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Test
  public void freeTest() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mFileSystem.getStatus(new AlluxioURI("/testFile")).getBlockIds().get(0);

    mFsShell.run("free", "/testFile");
    triggerWorkerHeartbeats(blockId);
    Assert.assertFalse(
        mFileSystem.getStatus(new AlluxioURI("/testFile")).getInMemoryPercentage() == 100);
  }

  @Test
  public void freeWildCardTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    long blockId =
        mFileSystem.getStatus(new AlluxioURI("/testWildCards/foo/foobar1")).getBlockIds().get(0);

    int ret = mFsShell.run("free", "/testWild*/foo/*");

    triggerWorkerHeartbeats(blockId);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar1"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar2"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/foobar4"));

    blockId =
        mFileSystem.getStatus(new AlluxioURI("/testWildCards/bar/foobar3")).getBlockIds().get(0);
    ret = mFsShell.run("free", "/testWild*/*/");
    triggerWorkerHeartbeats(blockId);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foobar4"));
  }

  // Execution of the blocks free needs two heartbeats.
  private void triggerWorkerHeartbeats(long blockId) {
    try {
      // Schedule 1st heartbeat from worker.
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
          TimeUnit.SECONDS));
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);

      // Waiting for the removal of blockMeta from worker.
      while (mLocalAlluxioCluster.getWorker().getBlockWorker().hasBlockMeta(blockId)) {
        CommonUtils.sleepMs(50);
      }

      // Schedule 2nd heartbeat from worker.
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
          TimeUnit.SECONDS));
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);

      // Ensure the 2nd heartbeat is finished.
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
          TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
