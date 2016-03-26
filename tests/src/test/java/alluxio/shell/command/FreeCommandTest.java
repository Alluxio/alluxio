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
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.util.CommonUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for free command.
 */
public class FreeCommandTest extends AbstractAlluxioShellTest {

  @BeforeClass
  public static void beforeClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_BLOCK_SYNC,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
  }

  @AfterClass
  public static void afterClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_BLOCK_SYNC,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
  }

  @Test
  public void freeTest() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);

    mFsShell.run("free", "/testFile");
    triggerWorkerHeartbeats();
    Assert.assertFalse(
        mFileSystem.getStatus(new AlluxioURI("/testFile")).getInMemoryPercentage() == 100);
  }

  @Test
  public void freeWildCardTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    int ret = mFsShell.run("free", "/testWild*/foo/*");
    triggerWorkerHeartbeats();
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar1"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar2"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/foobar4"));

    ret = mFsShell.run("free", "/testWild*/*/");
    triggerWorkerHeartbeats();
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foobar4"));
  }

  private void triggerWorkerHeartbeats() {
    // Execute the blocks free, which needs two heartbeats. Make sure there is some time delay
    // between two heartbeats to make sure worker got time to generate block removal reports.
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    try {
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
          TimeUnit.SECONDS));
      CommonUtils.sleepMs(50);
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
          TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
