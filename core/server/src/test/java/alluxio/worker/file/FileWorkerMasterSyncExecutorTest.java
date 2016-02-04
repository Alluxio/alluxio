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

package alluxio.worker.file;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import alluxio.thrift.FileSystemCommand;
import alluxio.worker.WorkerIdRegistry;

/**
 * Tests {@link FileWorkerMasterSyncExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileDataManager.class, FileSystemMasterClient.class})
public final class FileWorkerMasterSyncExecutorTest {
  private FileDataManager mFileDataManager;
  private FileSystemMasterClient mFileSystemMasterClient;
  private FileWorkerMasterSyncExecutor mFileWorkerMasterSyncExecutor;

  @Before
  public void before() {
    mFileDataManager = Mockito.mock(FileDataManager.class);
    mFileSystemMasterClient = Mockito.mock(FileSystemMasterClient.class);
    mFileWorkerMasterSyncExecutor =
        new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterClient);
  }

  /**
   * {@link FileDataManager.#clearPersistedFiles(java.util.List)} is not called when the heartbeat
   * of {@link FileSystemMasterClient} fails.
   */
  @Test
  public void heartbeatFailureTest() throws Exception {
    List<Long> persistedFiles = Lists.newArrayList(1L);
    Mockito.when(mFileDataManager.getPersistedFiles()).thenReturn(persistedFiles);
    // first time fails, second time passes
    Mockito.when(mFileSystemMasterClient.heartbeat(WorkerIdRegistry.getWorkerId(), persistedFiles))
        .thenThrow(new IOException("failure"));
    mFileWorkerMasterSyncExecutor.heartbeat();
    Mockito.verify(mFileDataManager, Mockito.never()).clearPersistedFiles(persistedFiles);
  }

  /**
   * Verifies {@link FileDataManager.#clearPersistedFiles(java.util.List)} is called when the
   * heartbeat is successful.
   */
  @Test
  public void heartbeatTest() throws Exception {
    List<Long> persistedFiles = Lists.newArrayList(1L);
    Mockito.when(mFileDataManager.getPersistedFiles()).thenReturn(persistedFiles);
    // first time fails, second time passes
    Mockito.when(mFileSystemMasterClient.heartbeat(WorkerIdRegistry.getWorkerId(), persistedFiles))
        .thenReturn(new FileSystemCommand());
    mFileWorkerMasterSyncExecutor.heartbeat();
    Mockito.verify(mFileDataManager).clearPersistedFiles(persistedFiles);
  }
}
