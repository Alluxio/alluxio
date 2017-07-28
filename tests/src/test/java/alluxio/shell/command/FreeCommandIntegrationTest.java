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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.AlluxioURI;
import alluxio.IntegrationTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.worker.block.BlockWorker;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for free command.
 */
public final class FreeCommandIntegrationTest extends AbstractAlluxioShellTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Test
  public void freeNonPersistedFile() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WriteType.MUST_CACHE, 10);
    // freeing non persisted files is expected to fail
    assertEquals(-1, mFsShell.run("free", fileName));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class));
    assertTrue(isInMemoryTest(fileName));
  }

  @Test
  public void freePinnedFile() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WriteType.CACHE_THROUGH, 10);
    mFsShell.run("pin", fileName);
    // freeing non persisted files is expected to fail
    assertEquals(-1, mFsShell.run("free", fileName));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class));
    assertTrue(isInMemoryTest(fileName));
  }

  @Test
  public void freePinnedFileForced() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WriteType.CACHE_THROUGH, 10);
    long blockId = mFileSystem.getStatus(new AlluxioURI(fileName)).getBlockIds().get(0);
    mFsShell.run("pin", fileName);
    assertEquals(0, mFsShell.run("free", "-f", fileName));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class), blockId);
    assertFalse(isInMemoryTest(fileName));
  }

  @Test
  public void free() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WriteType.CACHE_THROUGH, 10);
    long blockId = mFileSystem.getStatus(new AlluxioURI(fileName)).getBlockIds().get(0);
    assertEquals(0, mFsShell.run("free", fileName));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class), blockId);
    assertFalse(isInMemoryTest(fileName));
  }

  @Test
  public void freeWildCardNonPersistedFile() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem, WriteType.MUST_CACHE);
    assertEquals(-1, mFsShell.run("free", testDir + "/foo/*"));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class));
    // freeing non persisted files is expected to fail
    assertTrue(isInMemoryTest(testDir + "/foo/foobar1"));
    assertTrue(isInMemoryTest(testDir + "/foo/foobar2"));
    assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    assertTrue(isInMemoryTest(testDir + "/foobar4"));
  }

  @Test
  public void freeWildCardPinnedFile() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem, WriteType.CACHE_THROUGH);
    mFsShell.run("pin", testDir + "/foo/*");
    assertEquals(-1, mFsShell.run("free", testDir + "/foo/*"));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class));
    // freeing non pinned files is expected to fail without "-f"
    assertTrue(isInMemoryTest(testDir + "/foo/foobar1"));
    assertTrue(isInMemoryTest(testDir + "/foo/foobar2"));
  }

  @Test
  public void freeWildCardPinnedFileForced() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem, WriteType.CACHE_THROUGH);
    long blockId1 = mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar1")).getBlockIds()
        .get(0);
    long blockId2 =
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).getBlockIds().get(0);
    mFsShell.run("pin", testDir + "/foo/foobar1");
    assertEquals(0, mFsShell.run("free", "-f", testDir + "/foo/*"));
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class), blockId1, blockId2);
    assertFalse(isInMemoryTest(testDir + "/foo/foobar1"));
    assertFalse(isInMemoryTest(testDir + "/foo/foobar2"));
    assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    assertTrue(isInMemoryTest(testDir + "/foobar4"));
  }

  @Test
  public void freeWildCard() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem, WriteType.CACHE_THROUGH);
    long blockId1 =
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar1")).getBlockIds().get(0);
    long blockId2 =
        mFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).getBlockIds().get(0);

    int ret = mFsShell.run("free", testDir + "/foo/*");

    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class), blockId1, blockId2);
    assertEquals(0, ret);
    assertFalse(isInMemoryTest(testDir + "/foo/foobar1"));
    assertFalse(isInMemoryTest(testDir + "/foo/foobar2"));
    assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    assertTrue(isInMemoryTest(testDir + "/foobar4"));

    blockId1 = mFileSystem.getStatus(new AlluxioURI(testDir + "/bar/foobar3")).getBlockIds().get(0);
    blockId2 = mFileSystem.getStatus(new AlluxioURI(testDir + "/foobar4")).getBlockIds().get(0);

    ret = mFsShell.run("free", testDir + "/*/");
    IntegrationTestUtils.waitForBlocksToBeFreed(
        mLocalAlluxioCluster.getWorkerProcess().getWorker(BlockWorker.class), blockId1, blockId2);
    assertEquals(0, ret);
    assertFalse(isInMemoryTest(testDir + "/bar/foobar3"));
    assertFalse(isInMemoryTest(testDir + "/foobar4"));
  }
}
