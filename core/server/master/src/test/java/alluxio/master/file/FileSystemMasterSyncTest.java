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
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.MountContext;

import org.junit.Test;
import org.mockito.Mockito;

public class FileSystemMasterSyncTest extends FileSystemMasterTestBase {
  private final CreateFileContext mCreateOptions = CreateFileContext.mergeFrom(
          CreateFilePOptions.newBuilder().setRecursive(true))
      .setWriteType(WriteType.CACHE_THROUGH);

  InodeSyncStream.SyncStatus createSyncStream(
      AlluxioURI path, long syncInterval, DescendantType descendantType)
      throws Exception {
    FileSystemMasterCommonPOptions options = FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(syncInterval).build();
    return mFileSystemMaster.syncMetadata(mFileSystemMaster.createRpcContext(), path, options,
        descendantType, null, null);
  }

  Long[] syncSetup(AlluxioURI mountPath) throws Exception {
    String ufsMountPath = mUfsPath.newFolder("ufs").getAbsolutePath();
    mFileSystemMaster.mount(mountPath, new AlluxioURI(ufsMountPath),
        MountContext.create(MountPOptions.newBuilder()));
    Long[] currentTime = new Long[] {1L};
    Mockito.doAnswer(invocation -> currentTime[0]).when(mClock).millis();
    return currentTime;
  }

  void checkSyncTime(AlluxioURI path, long time, DescendantType descendantType)
      throws Exception {
    checkSyncTime(path, time, descendantType, 1);
  }

  void checkSyncTime(AlluxioURI path, long time, DescendantType descendantType, long interval)
      throws Exception {
    assertEquals(time, mFileSystemMaster.getSyncPathCache()
        .shouldSyncPath(path, interval, descendantType)
        .skippedSync().getLastSyncTime());
  }

  void checkNeedsSync(AlluxioURI path, DescendantType descendantType) throws Exception {
    assertTrue(mFileSystemMaster.getSyncPathCache().shouldSyncPath(
        path, 1, descendantType).isShouldSync());
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
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, DescendantType.NONE);
    checkSyncTime(f2, 1, DescendantType.NONE);
    checkSyncTime(dirPath, 1, DescendantType.ALL);
    // sync not needed
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(f2, 1, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // ensure the children don't need to be synced at time 2 with sync interval 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 2, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(f2, 2, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL);
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
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, DescendantType.NONE);
    checkSyncTime(f2, 1, DescendantType.NONE);
    checkSyncTime(dirPath, 1, DescendantType.ALL);

    // sync child f1 at time 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 2, DescendantType.NONE);
    checkNeedsSync(f2, DescendantType.NONE);
    checkNeedsSync(dirPath, DescendantType.ALL);

    // now sync the parent, at time 2 with interval 1, so f1 doesn't need a sync
    currentTime[0] = 2L;
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(2L, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(dirPath)
        .get().getSecond());
    assertEquals(2L, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(f1)
        .get().getFirst());
    checkSyncTime(f1, 2, DescendantType.NONE);
    checkSyncTime(f2, 2, DescendantType.NONE);
    checkSyncTime(dirPath, 2, DescendantType.ALL);

    // sync not needed at the same time
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // sync child f1 at time 3
    currentTime[0] = 3L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 3, DescendantType.NONE);
    checkNeedsSync(f2, DescendantType.NONE);
    checkNeedsSync(dirPath, DescendantType.ALL);

    // now sync the parent, at time 4 with interval 2, so f1 doesn't need a sync
    // but the parent only gets updated to time 3
    currentTime[0] = 4L;
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(3, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(dirPath)
        .get().getSecond());
    assertEquals(3, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(f1)
        .get().getFirst());
    checkNeedsSync(f1, DescendantType.NONE);
    checkNeedsSync(f2, DescendantType.NONE);
    checkNeedsSync(dirPath, DescendantType.ALL);
    currentTime[0] = 3L;
    checkSyncTime(f1, 3, DescendantType.NONE);
    checkSyncTime(f2, 3, DescendantType.NONE);
    checkSyncTime(dirPath, 3, DescendantType.ALL);
    currentTime[0] = 4L;

    // sync parent at time 4 with interval 1 so all should sync
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(4, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(dirPath)
        .get().getSecond());
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
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, DescendantType.NONE);
    checkSyncTime(f2, 1, DescendantType.NONE);
    checkSyncTime(dirPath, 1, DescendantType.ALL);

    // sync child f1 at time 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 2, DescendantType.NONE);
    checkNeedsSync(f2, DescendantType.NONE);
    checkNeedsSync(dirPath, DescendantType.ALL);

    // now sync the parent, at time 2 with interval 1, so f1 doesn't need a sync
    currentTime[0] = 2L;
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(2, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(dirPath)
        .get().getSecond());
    assertEquals(2, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(f1)
        .get().getFirst());
    checkSyncTime(f1, 2, DescendantType.NONE);
    checkSyncTime(f2, 2, DescendantType.NONE);
    checkSyncTime(dirPath, 2, DescendantType.ALL);

    // sync not needed at the same time
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // sync child f1 at time 3
    currentTime[0] = 3L;
    syncStatus = createSyncStream(f1, 1, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 3, DescendantType.NONE);
    checkNeedsSync(f2, DescendantType.NONE);
    checkNeedsSync(dirPath, DescendantType.ALL);

    // now sync the parent, at time 4 with interval 2, so f1 doesn't need a sync
    // but the sync still happens at time 4 because the file is loaded from the UFS
    // when the directory is listed
    currentTime[0] = 4L;
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    assertEquals(4, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(dirPath)
        .get().getSecond());
    checkSyncTime(f1, 4, DescendantType.NONE);
    checkSyncTime(f2, 4, DescendantType.NONE);
    checkSyncTime(dirPath, 4, DescendantType.ALL);

    // sync parent at time 4 with interval 1, so sync should not be needed
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
  }

  /**
   * Sync a nested file with a none descendant type.
   * The sync should still be valid when doing a sync on the
   * parent directory of a recursive type since the child is a file.
   */
  @Test
  public void syncNestedFile() throws Exception {
    AlluxioURI mountPath = new AlluxioURI("/mount");
    Long[] currentTime = syncSetup(mountPath);
    AlluxioURI dirPath = mountPath.join("dir");
    AlluxioURI f1 = dirPath.join("f1");
    createFileWithSingleBlock(f1, mCreateOptions);
    AlluxioURI dir1 = dirPath.join("dir1");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryContext.defaults());

    // sync the directory recursively at time 1
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, DescendantType.ALL);
    checkSyncTime(dirPath, 1, DescendantType.ALL);

    // sync not needed on any child
    syncStatus = createSyncStream(f1, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dir1, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // sync f1 and nested dir1 at time 2
    currentTime[0] = 2L;
    syncStatus = createSyncStream(f1, 0, DescendantType.NONE);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    syncStatus = createSyncStream(dir1, 0, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);

    // sync the directory at time 3, but not recursively
    currentTime[0] = 3L;
    syncStatus = createSyncStream(dirPath, 0, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    // f1 should have a sync time of 3 even for a descendant type all since it is a file
    checkSyncTime(f1, 3, DescendantType.ALL);
    // nested dir1 should only have a sync time of 3 for a none descendant type
    checkSyncTime(dir1, 3, DescendantType.NONE);
    // for type all it should have a sync time of 2 from the previous sync
    checkSyncTime(dir1, 2, DescendantType.ALL, 2);

    // ensure the children don't need to be synced at time 3 with sync interval 2
    syncStatus = createSyncStream(f1, 2, DescendantType.ONE);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dir1, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
  }

  /**
   * Test a combination of sync and invalidations.
   */
  @Test
  public void syncInvalidation() throws Exception {
    AlluxioURI mountPath = new AlluxioURI("/mount");
    Long[] currentTime = syncSetup(mountPath);
    AlluxioURI dirPath = mountPath.join("dir");
    AlluxioURI f1 = dirPath.join("f1");
    createFileWithSingleBlock(f1, mCreateOptions);
    AlluxioURI dir1 = dirPath.join("dir1");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryContext.defaults());

    // sync the directory recursively at time 1
    InodeSyncStream.SyncStatus syncStatus = createSyncStream(dirPath, 0, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    checkSyncTime(f1, 1, DescendantType.ALL);
    checkSyncTime(dirPath, 1, DescendantType.ALL);

    // sync not needed on any child
    syncStatus = createSyncStream(f1, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dir1, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 1, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);

    // invalidate f1 at time 1
    mFileSystemMaster.invalidateSyncPath(f1);
    // move to time 2
    currentTime[0] = 2L;
    // dir1 should still not need a sync with interval 2
    syncStatus = createSyncStream(dir1, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    // f1 and its parent should need a sync even with a large interval
    syncStatus = createSyncStream(f1, 100, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    syncStatus = createSyncStream(dirPath, 100, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.OK, syncStatus);
    // the sync time for dirPath should be 1, since child dir1 would have
    // not been synced, so the sync time will only be updated to time 1
    assertEquals(1, (long) mFileSystemMaster.getSyncPathCache().getSyncTimesForPath(dirPath)
        .get().getSecond());

    // now none should need a sync with interval 2
    syncStatus = createSyncStream(f1, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dir1, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
    syncStatus = createSyncStream(dirPath, 2, DescendantType.ALL);
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, syncStatus);
  }
}
