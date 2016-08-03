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

package alluxio.shell.command;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Test for GetFilesPersistInProgress command.
 */
public class GetFilesPersistInProgressCommandTest extends AbstractAlluxioShellTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);

  private static final String DESCRIPTION =
      "Files that are currently persisted in progress on worker: \n";

  private UnderFileSystem mUfs;

  @Before
  public void setup() {
    String ufsAddress = Configuration.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(ufsAddress);
  }

  @Test
  public void testGetFilesPersistInProgressCommand() throws Exception {
    final String testFilePath = "/testFile";

    FileSystemTestUtils.createByteFile(mFileSystem, testFilePath, WriteType.ASYNC_THROUGH, 10);
    runCommandAndVerify(DESCRIPTION);

    // Schedule 1st heartbeat from worker.
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5, TimeUnit.SECONDS);

    runCommandAndVerify(DESCRIPTION + testFilePath + "\n");

    // Wait for async persistence to finish.
    CommonTestUtils.waitFor("Wait file persisted to under file system",
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            String ufsRoot = Configuration.get(Constants.UNDERFS_ADDRESS);
            String dstPath = PathUtils.concatPath(ufsRoot, testFilePath);

            try {
              return mUfs.exists(dstPath);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }, 100 * Constants.SECOND_MS);

    // Schedule 2nd heartbeat from worker.
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5, TimeUnit.SECONDS);

    runCommandAndVerify(DESCRIPTION);
  }

  private void runCommandAndVerify(String expectedOutput) {
    int ret = mFsShell.run("getFilesPersistInProgress");
    Assert.assertEquals(0, ret);

    Assert.assertEquals(mOutput.toString(), expectedOutput);
    mOutput.reset();
  }
}
