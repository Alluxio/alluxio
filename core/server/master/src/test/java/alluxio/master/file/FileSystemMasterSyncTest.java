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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.UfsSyncPathCache;

import com.google.common.cache.Cache;
import org.junit.Test;
import org.mockito.Mockito;

public class FileSystemMasterSyncTest extends FileSystemMasterTestBase {
  private final CreateFileContext mCreateOptions = CreateFileContext.mergeFrom(
          CreateFilePOptions.newBuilder().setRecursive(true)
              .setWriteType(WritePType.CACHE_THROUGH))
      .setWriteType(WriteType.CACHE_THROUGH);

  InodeSyncStream.SyncStatus createSyncStream(
      AlluxioURI path, long syncInterval, DescendantType descendantType, boolean isGetFileInfo)
      throws Exception {
    FileSystemMasterCommonPOptions options = FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(syncInterval).build();
    return mFileSystemMaster.syncMetadata(mFileSystemMaster.createRpcContext(), path, options,
        descendantType, null, null, isGetFileInfo);
  }

  Long[] syncSetup(AlluxioURI mountPath) throws Exception {
    String ufsMountPath = mUfsPath.newFolder("ufs").getAbsolutePath();
    mFileSystemMaster.mount(mountPath, new AlluxioURI(ufsMountPath),
        MountContext.create(MountPOptions.newBuilder()));
    Long[] currentTime = new Long[] {1L};
    Mockito.doAnswer(invocation -> currentTime[0]).when(mClock).millis();
    return currentTime;
  }

  void checkSyncTime(AlluxioURI path, long time, boolean isGetFileInfo) {
    assertEquals(time, mFileSystemMaster.getSyncPathCache()
        .shouldSyncPath(path.getPath(), 1, isGetFileInfo)
        .skippedSync().getLastSyncTime());
  }

  void checkNeedsSync(AlluxioURI path, boolean isGetFileInfo) {
    assertTrue(mFileSystemMaster.getSyncPathCache().shouldSyncPath(
        path.getPath(), 1, isGetFileInfo).isShouldSync());
  }

  @Test
  public void syncDelete() throws Exception {
    AlluxioURI mountPath = new AlluxioURI("/mount");
    Long[] currentTime = syncSetup(mountPath);
    AlluxioURI dirPath = mountPath.join("dir");
    AlluxioURI f1 = dirPath.join("f1");
    createFileWithSingleBlock(f1, mCreateOptions);

    currentTime[0]++;
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);

    deleteFileOutsideOfAlluxio(f1);
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    mFileSystemMaster.getFileInfo(f1, GetStatusContext.defaults());

    currentTime[0]++;
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertThrows(FileDoesNotExistException.class,
        () -> mFileSystemMaster.getFileInfo(f1, GetStatusContext.defaults()));
  }

  @Test
  public void syncDir() throws Exception {
    AlluxioURI mountPath = new AlluxioURI("/mount");
    Long[] currentTime = syncSetup(mountPath);
    AlluxioURI dirPath = mountPath.join("dir");
    AlluxioURI f1 = dirPath.join("f1");
    AlluxioURI f2 = dirPath.join("f2");
    createFileWithSingleBlock(f1, mCreateOptions);
    createFileWithSingleBlock(f2, mCreateOptions);

    // sync the directory recursively at time 1
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, true);
    checkSyncTime(f2, 1, true);
    checkSyncTime(dirPath, 1, false);
    // sync not needed
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(f2, 1, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // ensure the children don't need to be synced at time 2 with sync interval 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 2, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(f2, 2, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
  }

  /**
   * Check that when recursively syncing a directory, if a child directory has been synced more
   * recently than the root sync directory and does not need a sync, then the child directory
   * is not synced, and the parent's sync time is updated to the time of the child sync.
   */
  @Test
  public void syncDirChild() throws Exception {
    AlluxioURI mountPath = new AlluxioURI("/mount");
    Long[] currentTime = syncSetup(mountPath);
    AlluxioURI dirPath = mountPath.join("dir");
    AlluxioURI f1 = dirPath.join("f1");
    AlluxioURI f2 = dirPath.join("f2");
    mFileSystemMaster.createDirectory(f1, CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder().setRecursive(true))
        .setWriteType(WriteType.CACHE_THROUGH));
    createFileWithSingleBlock(f2, mCreateOptions);

    // sync the directory recursively at time 1
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, true);
    checkSyncTime(f2, 1, true);
    checkSyncTime(dirPath, 1, false);

    // sync child f1 at time 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 2, true);
    checkNeedsSync(f2, true);
    checkNeedsSync(dirPath, false);

    // now sync the parent, at time 2 with interval 1, so f1 doesn't need a sync
    currentTime[0] = 2L;
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    Cache<String, UfsSyncPathCache.SyncTime> syncCache =
        mFileSystemMaster.getSyncPathCache().getCache();
    assertEquals(2, syncCache.getIfPresent(dirPath.getPath()).getLastRecursiveSyncMs());
    assertEquals(2, syncCache.getIfPresent(f1.getPath()).getLastSyncMs());
    checkSyncTime(f1, 2, true);
    checkSyncTime(f2, 2, true);
    checkSyncTime(dirPath, 2, false);

    // sync not needed at the same time
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // sync child f1 at time 3
    currentTime[0] = 3L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 3, true);
    checkNeedsSync(f2, true);
    checkNeedsSync(dirPath, false);

    // now sync the parent, at time 4 with interval 2, so f1 doesn't need a sync
    // but the parent only gets updated to time 3
    currentTime[0] = 4L;
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(3, syncCache.getIfPresent(dirPath.getPath()).getLastRecursiveSyncMs());
    assertEquals(3, syncCache.getIfPresent(f1.getPath()).getLastSyncMs());
    checkNeedsSync(f1, true);
    checkNeedsSync(f2, true);
    checkNeedsSync(dirPath, false);
    currentTime[0] = 3L;
    checkSyncTime(f1, 3, true);
    checkSyncTime(f2, 3, true);
    checkSyncTime(dirPath, 3, false);
    currentTime[0] = 4L;

    // sync parent at time 4 with interval 1 so all should sync
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(4, syncCache.getIfPresent(dirPath.getPath()).getLastRecursiveSyncMs());
  }

  /**
   * This follows the same structure as {@link FileSystemMasterSyncTest#syncDirChild()}
   * except the updated child is a file instead of a directory, in this case the child
   * will be synced, because its status has already been loaded from the UFS when listing
   * the root sync directory.
   */
  @Test
  public void syncNestedFileChild() throws Exception {
    // ACL needs to be disabled, otherwise the child sync will be skipped because
    // loading the ACL would require an extra UFS operation
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);
    AlluxioURI mountPath = new AlluxioURI("/mount");
    Long[] currentTime = syncSetup(mountPath);
    AlluxioURI dirPath = mountPath.join("dir");
    AlluxioURI f1 = dirPath.join("f1");
    AlluxioURI f2 = dirPath.join("f2");
    createFileWithSingleBlock(f1, mCreateOptions);
    createFileWithSingleBlock(f2, mCreateOptions);

    // sync the directory recursively at time 1
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, true);
    checkSyncTime(f2, 1, true);
    checkSyncTime(dirPath, 1, false);

    // sync child f1 at time 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 2, true);
    checkNeedsSync(f2, true);
    checkNeedsSync(dirPath, false);

    // now sync the parent, at time 2 with interval 1, so f1 doesn't need a sync
    currentTime[0] = 2L;
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    Cache<String, UfsSyncPathCache.SyncTime> syncCache =
        mFileSystemMaster.getSyncPathCache().getCache();
    assertEquals(2, syncCache.getIfPresent(dirPath.getPath()).getLastRecursiveSyncMs());
    assertEquals(2, syncCache.getIfPresent(f1.getPath()).getLastSyncMs());
    checkSyncTime(f1, 2, true);
    checkSyncTime(f2, 2, true);
    checkSyncTime(dirPath, 2, false);

    // sync not needed at the same time
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // sync child f1 at time 3
    currentTime[0] = 3L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE, true);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 3, true);
    checkNeedsSync(f2, true);
    checkNeedsSync(dirPath, false);

    // now sync the parent, at time 4 with interval 2, so f1 doesn't need a sync
    // but the sync still happens at time 4 because the file is loaded from the UFS
    // when the directory is listed
    currentTime[0] = 4L;
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(4, syncCache.getIfPresent(dirPath.getPath()).getLastRecursiveSyncMs());
    checkSyncTime(f1, 4, true);
    checkSyncTime(f2, 4, true);
    checkSyncTime(dirPath, 4, false);

    // sync parent at time 4 with interval 1, so sync should not be needed
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL, false);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
  }
}
