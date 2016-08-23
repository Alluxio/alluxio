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

import alluxio.AlluxioURI;
import alluxio.IntegrationTestUtils;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.AlluxioException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for free command.
 */
public final class FreeCommandTest extends AbstractAlluxioShellTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Test
  public void free() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WriteType.MUST_CACHE, 10);
    long blockId = mFileSystem.getStatus(new AlluxioURI(fileName)).getBlockIds().get(0);

    mFsShell.run("free", fileName);
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorker().getBlockWorker(), blockId);
    Assert.assertFalse(isInMemoryTest(fileName));
  }

  @Test
  public void freeWildCard() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    long blockId1 =
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar1")).getBlockIds().get(0);
    long blockId2 =
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).getBlockIds().get(0);

    int ret = mFsShell.run("free", testDir + "/foo/*");

    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorker().getBlockWorker(), blockId1, blockId2);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest(testDir + "/foo/foobar1"));
    Assert.assertFalse(isInMemoryTest(testDir + "/foo/foobar2"));
    Assert.assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    Assert.assertTrue(isInMemoryTest(testDir + "/foobar4"));

    blockId1 = mFileSystem.getStatus(new AlluxioURI(testDir + "/bar/foobar3")).getBlockIds().get(0);
    blockId2 = mFileSystem.getStatus(new AlluxioURI(testDir + "/foobar4")).getBlockIds().get(0);

    ret = mFsShell.run("free", testDir + "/*/");
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorker().getBlockWorker(), blockId1, blockId2);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest(testDir + "/bar/foobar3"));
    Assert.assertFalse(isInMemoryTest(testDir + "/foobar4"));
  }

}
