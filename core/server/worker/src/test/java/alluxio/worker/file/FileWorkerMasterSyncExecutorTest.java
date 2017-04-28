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

package alluxio.worker.file;

import alluxio.exception.status.UnavailableException;
import alluxio.thrift.FileSystemCommand;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
    mFileWorkerMasterSyncExecutor = new FileWorkerMasterSyncExecutor(mFileDataManager,
        mFileSystemMasterClient, new AtomicReference<>(10L));
  }

  /**
   * {@link FileDataManager#clearPersistedFiles(java.util.List)} is not called when the heartbeat
   * of {@link FileSystemMasterClient} fails.
   */
  @Test
  public void heartbeatFailure() throws Exception {
    List<Long> persistedFiles = Lists.newArrayList(1L);
    Mockito.when(mFileDataManager.getPersistedFiles()).thenReturn(persistedFiles);
    // first time fails, second time passes
    Mockito.when(mFileSystemMasterClient.heartbeat(Mockito.anyLong(), Mockito.eq(persistedFiles)))
        .thenThrow(new UnavailableException("failure"));
    mFileWorkerMasterSyncExecutor.heartbeat();
    Mockito.verify(mFileDataManager, Mockito.never()).clearPersistedFiles(persistedFiles);
  }

  /**
   * Verifies {@link FileDataManager#clearPersistedFiles(java.util.List)} is called when the
   * heartbeat is successful.
   */
  @Test
  public void heartbeat() throws Exception {
    List<Long> persistedFiles = Lists.newArrayList(1L);
    Mockito.when(mFileDataManager.getPersistedFiles()).thenReturn(persistedFiles);
    // first time fails, second time passes
    Mockito.when(mFileSystemMasterClient.heartbeat(Mockito.anyLong(), Mockito.eq(persistedFiles)))
        .thenReturn(new FileSystemCommand());
    mFileWorkerMasterSyncExecutor.heartbeat();
    Mockito.verify(mFileDataManager).clearPersistedFiles(persistedFiles);
  }
}
