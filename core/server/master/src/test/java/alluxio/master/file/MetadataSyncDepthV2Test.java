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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.mdsync.BaseTask;
import alluxio.master.file.mdsync.SyncOperation;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MetadataSyncDepthV2Test extends MetadataSyncV2TestBase {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {DirectoryLoadType.SINGLE_LISTING, DescendantType.ALL},
        {DirectoryLoadType.BFS, DescendantType.ALL},
        {DirectoryLoadType.DFS, DescendantType.ALL},
        {DirectoryLoadType.SINGLE_LISTING, DescendantType.ONE},
        {DirectoryLoadType.BFS, DescendantType.ONE},
        {DirectoryLoadType.DFS, DescendantType.ONE},
        {DirectoryLoadType.SINGLE_LISTING, DescendantType.NONE},
        {DirectoryLoadType.BFS, DescendantType.NONE},
        {DirectoryLoadType.DFS, DescendantType.NONE},
    });
  }

  DescendantType mDescendantType;

  public MetadataSyncDepthV2Test(
      DirectoryLoadType directoryLoadType, DescendantType descendantType) {
    mDescendantType = descendantType;
    mDirectoryLoadType = directoryLoadType;
  }

  @Test
  public void syncSingleDir() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/", "");

    // Sync the dir
    AlluxioURI syncPath = MOUNT_POINT.join(TEST_DIRECTORY);
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
  }

  @Test
  public void syncSingleDirNested() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    String dirPath = TEST_DIRECTORY + "/" + TEST_DIRECTORY + "/";
    mS3Client.putObject(TEST_BUCKET, dirPath, "");

    // Sync the dir
    AlluxioURI syncPath = MOUNT_POINT.join(TEST_DIRECTORY).join(TEST_DIRECTORY);
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 2L
    ));

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));

    // Delete the dir
    mS3Client.deleteObject(TEST_BUCKET, dirPath);
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 1L
    ));

    // The parent should also be gone
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join(TEST_DIRECTORY), mDescendantType, mDirectoryLoadType, 0)
        .getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 1L
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());

    // Sync the root, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of());
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void syncSingleFile() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE, TEST_CONTENT);

    // Sync the file
    AlluxioURI syncPath = MOUNT_POINT.join(TEST_DIRECTORY).join(TEST_FILE);
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 2L
    ));

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));

    // update the metadata for the path
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE, TEST_CONTENT_MODIFIED);

    // Sync should see the change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.RECREATE, 1L
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());

    // Delete the file
    mS3Client.deleteObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE);
    // Sync the root, all should be removed
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, mDescendantType, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, mDescendantType == DescendantType.NONE ? 0L : 2L
    ));
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    boolean exists = mFileSystemMaster.exists(syncPath, existsNoSync());
    if (mDescendantType == DescendantType.NONE) {
      // since we only synced the root path, the nested file should not be deleted
      assertTrue(exists);
    } else {
      assertFalse(exists);
    }
  }
}
