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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.exception.status.UnavailableException;
import alluxio.grpc.FileSystemCommand;
import alluxio.grpc.FileSystemHeartbeatPOptions;
import alluxio.thrift.FileSystemHeartbeatTOptions;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
    mFileDataManager = mock(FileDataManager.class);
    mFileSystemMasterClient = mock(FileSystemMasterClient.class);
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
    List<String> ufsFingerprintList = Lists.newArrayList("ufs fingerprint");
    FileDataManager.PersistedFilesInfo filesInfo =
        new FileDataManager.PersistedFilesInfo(persistedFiles, ufsFingerprintList);
    when(mFileDataManager.getPersistedFileInfos()).thenReturn(filesInfo);
    // first time fails, second time passes
    when(mFileSystemMasterClient.heartbeat(anyLong(), eq(persistedFiles),
        any(FileSystemHeartbeatPOptions.class)))
        .thenThrow(new UnavailableException("failure"));
    mFileWorkerMasterSyncExecutor.heartbeat();
    verify(mFileDataManager, never()).clearPersistedFiles(persistedFiles);
  }

  /**
   * Verifies {@link FileDataManager#clearPersistedFiles(java.util.List)} is called when the
   * heartbeat is successful.
   */
  @Test
  public void heartbeat() throws Exception {
    List<Long> persistedFiles = Lists.newArrayList(1L);
    List<String> ufsFingerprintList = Lists.newArrayList("ufs fingerprint");
    FileDataManager.PersistedFilesInfo filesInfo =
        new FileDataManager.PersistedFilesInfo(persistedFiles, ufsFingerprintList);
    when(mFileDataManager.getPersistedFileInfos()).thenReturn(filesInfo);
    // first time fails, second time passes
    when(mFileSystemMasterClient.heartbeat(anyLong(), eq(persistedFiles)))
        .thenReturn(FileSystemCommand.newBuilder().build());
    mFileWorkerMasterSyncExecutor.heartbeat();
    verify(mFileDataManager).clearPersistedFiles(persistedFiles);
  }
}
