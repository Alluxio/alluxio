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

package tachyon.client;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.IntegrationTestUtils;
import tachyon.TachyonURI;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.master.file.meta.PersistenceState;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link tachyon.client.file.FileOutStream} of under storage type being async
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
    FileOutStream os = mTfs.getOutStream(filePath, mWriteAsync);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    TachyonFile file = mTfs.open(filePath);
    FileInfo fileInfo = mTfs.getInfo(file);
    Assert.assertEquals(PersistenceState.IN_PROGRESS.toString(), fileInfo.getPersistenceState());
    Assert.assertTrue(fileInfo.isCompleted);

    // execute the async persist, which needs two heartbeats
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
        TimeUnit.SECONDS));

    // sleep and wait for worker to persist the file
    IntegrationTestUtils.waitForPersist(mLocalTachyonClusterResource, file.getFileId());

    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
        TimeUnit.SECONDS));

    fileInfo = mTfs.getInfo(file);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), fileInfo.getPersistenceState());

    checkWrite(filePath, mWriteAsync.getUnderStorageType(), length, length);
  }
}
