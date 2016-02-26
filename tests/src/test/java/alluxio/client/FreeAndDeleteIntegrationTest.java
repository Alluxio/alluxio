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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.master.PrivateAccess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.io.PathUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for file free and delete with under storage persisted.
 *
 */
public final class FreeAndDeleteIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = new LocalAlluxioClusterResource(
      WORKER_CAPACITY_BYTES, 100 * Constants.MB,
      Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    Configuration workerConfiguration = mLocalAlluxioClusterResource.get().getWorkerConf();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(workerConfiguration);
  }

  @BeforeClass
  public static void beforeClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_BLOCK_SYNC,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
  }

  @AfterClass
  public static void afterClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_BLOCK_SYNC,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
  }

  @Test
  public void freeAndDeleteIntegrationTest() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, mWriteBoth);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    mFileSystem.free(filePath);
    // Execute the blocks free, which needs two heartbeats
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    // Block metadata is still present after block freed.
    Assert.assertEquals(1, status.getBlockIds().size());

    mFileSystem.delete(filePath);
    // File is immediately gone after delete.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystem.getStatus(filePath);

    // Execute the lost files detection.
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_FILES_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5,
        TimeUnit.SECONDS));

    // Verify the blocks are not in mLostBlocks.
    BlockMaster bm = PrivateAccess.getBlockMaster(mLocalAlluxioClusterResource.get().getMaster()
        .getInternalMaster());
    Assert.assertEquals(0, bm.getLostBlocks().size());
  }
}
