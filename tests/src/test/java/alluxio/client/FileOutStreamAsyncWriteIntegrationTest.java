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

package alluxio.client;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import alluxio.IntegrationTestUtils;
import alluxio.TachyonURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream} of under storage type being async
 * persist.
 *
 */
public final class FileOutStreamAsyncWriteIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {

  @BeforeClass
  public static void beforeClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
  }

  @Test
  public void asyncWriteTest() throws Exception {
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
        TimeUnit.SECONDS));

    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath, mWriteAsync);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.IN_PROGRESS.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    // execute the async persist, which needs two heartbeats
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
        TimeUnit.SECONDS));

    IntegrationTestUtils.waitForPersist(mLocalTachyonClusterResource, status.getFileId());

    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
        TimeUnit.SECONDS));

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkWrite(filePath, mWriteAsync.getUnderStorageType(), length, length);
  }
}
