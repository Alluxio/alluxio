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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.metasync.SyncFailReason;
import alluxio.master.file.metasync.SyncOperation;
import alluxio.master.file.metasync.TestMetadataSyncer;
import alluxio.master.mdsync.BaseTask;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
@RunWith(Parameterized.class)
public class FileSystemMetadataSyncV2Test extends MetadataSyncV2TestBase {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {DirectoryLoadType.SINGLE_LISTING},
        {DirectoryLoadType.BFS},
        {DirectoryLoadType.DFS},
    });
  }

  public FileSystemMetadataSyncV2Test(DirectoryLoadType directoryLoadType) {
    mDirectoryLoadType = directoryLoadType;
  }

  @Test
  public void syncDirDepth() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE, TEST_CONTENT);

    // Sync the dir
    AlluxioURI syncPath = MOUNT_POINT.join(TEST_DIRECTORY);
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, DescendantType.NONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, DescendantType.NONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));

    // Sync with depth 1, should see the file
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, DescendantType.NONE, mDirectoryLoadType, 0).getBaseTask();
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
  public void basicSyncMultiRequest() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_FILE + i, TEST_CONTENT);
    }
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 11L
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET,
        "", mFileSystemMaster, mClient);

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 11L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET,
        "", mFileSystemMaster, mClient);
  }

  @Test
  public void dirTest() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE, TEST_CONTENT);

    // load the dir with depth 1
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    List<FileInfo> items = mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(true));
    assertEquals(1, items.size());
  }

  @Test
  public void basicSync() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));

    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET,
        "", mFileSystemMaster, mClient);

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET,
        "", mFileSystemMaster, mClient);
  }

  @Test
  public void basicSyncNestedMount() throws Throwable {
    mS3Client.putObject(TEST_BUCKET,
        TEST_DIRECTORY + "/", "");
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT.join(TEST_DIRECTORY), MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE, TEST_CONTENT);

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, TEST_DIRECTORY, mFileSystemMaster, mClient);
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));

    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, TEST_DIRECTORY, mFileSystemMaster, mClient);
  }

  @Test
  public void basicSyncNestedMountNestedDir() throws Throwable {
    mS3Client.putObject(TEST_BUCKET,
        TEST_DIRECTORY + "/", "");
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT.join(TEST_DIRECTORY), MountContext.defaults());
    // create files
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE + i, TEST_CONTENT);
    }
    // create nested files
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/"
          + TEST_DIRECTORY + "/" + TEST_FILE + i, TEST_CONTENT);
    }

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 21L
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, TEST_DIRECTORY, mFileSystemMaster, mClient);

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP,
        mDirectoryLoadType == DirectoryLoadType.SINGLE_LISTING ? 20L : 21L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, TEST_DIRECTORY, mFileSystemMaster, mClient);
  }

  @Test
  public void basicSyncNestedMountNestedDirWithMarkers() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    // create directory markers
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/", "");
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_DIRECTORY + "/", "");
    // create files
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE + i, TEST_CONTENT);
    }
    // create nested files
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/"
          + TEST_DIRECTORY + "/" + TEST_FILE + i, TEST_CONTENT);
    }

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 22L
    ));

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 22L
    ));

    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void basicSyncEmptyDirWithMarkers() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    // create directory marker
    mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/", "");

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L,
        SyncOperation.NOOP, 0L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void basicSyncNestedFile() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE + i, TEST_CONTENT);
    }

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 11L
    ));
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP,
        mDirectoryLoadType != DirectoryLoadType.SINGLE_LISTING ? 11L : 10L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void basicSyncDirectory() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    for (int i = 0; i < 10; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_DIRECTORY + "/" + TEST_FILE + i, TEST_CONTENT);
    }

    AlluxioURI syncPath = MOUNT_POINT.join(TEST_DIRECTORY);
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 11L
    ));

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        syncPath, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 10L
    ));
  }

  @Test
  public void syncInodeHappyPath() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());

    // Sync one file from UFS
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join(TEST_FILE), DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));
    FileInfo info = mFileSystemMaster.getFileInfo(MOUNT_POINT.join(TEST_FILE), getNoSync());
    assertFalse(info.isFolder());
    assertTrue(info.isCompleted());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join(TEST_FILE), DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    // Delete the file from UFS, then sync again
    mS3Client.deleteObject(TEST_BUCKET, TEST_FILE);
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join(TEST_FILE), DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 1L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
  }

  @Test
  public void syncInodeDescendantTypeNoneHappyPath() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());

    // Sync one file from UFS
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join(TEST_FILE), DescendantType.NONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));
    FileInfo info = mFileSystemMaster.getFileInfo(MOUNT_POINT.join(TEST_FILE), getNoSync());
    assertFalse(info.isFolder());
    assertTrue(info.isCompleted());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void deleteOneAndAddAnother() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "foo/a", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "foo/c", TEST_CONTENT);

    // Sync two files from UFS
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join("foo"), DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 3L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    // Delete one and create another
    mS3Client.deleteObject(TEST_BUCKET, "foo/a");
    mS3Client.putObject(TEST_BUCKET, "foo/b", TEST_CONTENT);
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join("foo"), DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L,
        SyncOperation.DELETE, 1L,
        SyncOperation.NOOP, 1L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void deleteDirectory() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "d1/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d1/f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d2/f1", TEST_CONTENT);

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 5L
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    mS3Client.deleteObject(TEST_BUCKET, "d1/f1");
    mS3Client.deleteObject(TEST_BUCKET, "d1/f2");
    mS3Client.putObject(TEST_BUCKET, "d0/f1", TEST_CONTENT);
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());

    // "d2/f1"
    long noopCount = 1;
    if (mDirectoryLoadType != DirectoryLoadType.SINGLE_LISTING) {
      // "d2"
      noopCount++;
    }
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 2L,
        SyncOperation.DELETE, 3L,
        SyncOperation.NOOP, noopCount
    ));

    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void syncInodeHappyPathNestedObjects() throws Throwable {
    mS3Client.putObject(TEST_BUCKET, "d1/1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d1/2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d1/3", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d2/1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d2/2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d2/3", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d3/1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d3/2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d3/3", TEST_CONTENT);
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());

    // count the files
    long numInodes = 9;
    // count the directories
    numInodes += 3;

    // Sync one file from UFS
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, numInodes
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    // count the files
    long noopCount = 9;
    if (mDirectoryLoadType != DirectoryLoadType.SINGLE_LISTING) {
      // count the directories
      noopCount += 3;
    }

    // Sync again, expect no change
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, noopCount
    ));
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
  }

  @Test
  public void syncNestedObjectsCreateThenDelete() throws Throwable {
    mS3Client.putObject(TEST_BUCKET, "d/1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d/2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d/3", TEST_CONTENT);
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());

    // count the files
    long numInodes = 3;
    // count the directories
    numInodes += 1;

    // Sync one file from UFS
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, numInodes
    ));
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    mS3Client.deleteObject(TEST_BUCKET, "d/1");
    mS3Client.deleteObject(TEST_BUCKET, "d/2");
    mS3Client.deleteObject(TEST_BUCKET, "d/3");

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 4L
    ));
  }

  @Test
  public void syncInodeUfsDown()
      throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    stopS3Server();
    final BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    assertThrows(IOException.class, () -> {
      result.waitComplete(TIMEOUT_MS);
    });
    assertSyncFailureReason(result.getTaskInfo(), SyncFailReason.LOADING_UFS_IO_FAILURE);

    startS3Server();
  }

  @Test
  public void syncInodeProcessingErrorHandling()
      throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();
    syncer.beforePerformSyncOne((ignored) -> {
      throw new Exception("fail");
    });
    final BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    assertThrows(Exception.class, () -> {
      result.waitComplete(TIMEOUT_MS);
    });
    assertSyncFailureReason(result.getTaskInfo(), SyncFailReason.PROCESSING_UNKNOWN);
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertFalse(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());

    syncer.beforePerformSyncOne((context) -> {
      Exception e = new Exception("fail");
      context.reportSyncFailReason(SyncFailReason.PROCESSING_CONCURRENT_UPDATE_DURING_SYNC, e);
      throw e;
    });
    final BaseTask result2 = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    assertThrows(Exception.class, () -> {
      result2.waitComplete(TIMEOUT_MS);
    });
    assertSyncFailureReason(result2.getTaskInfo(),
        SyncFailReason.PROCESSING_CONCURRENT_UPDATE_DURING_SYNC);
  }

  @Test
  public void syncDirectoryHappyPath() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "file1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "file2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "file3", TEST_CONTENT);

    // To recreate -> content hashes are different
    mFileSystemMaster.createFile(MOUNT_POINT.join("file1"), CreateFileContext.defaults());
    mFileSystemMaster.completeFile(MOUNT_POINT.join("file1"), CompleteFileContext.defaults());

    // To delete -> doesn't exist in UFS
    mFileSystemMaster.createDirectory(MOUNT_POINT.join("directory1"),
        CreateDirectoryContext.defaults());

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        // file2 & file 3
        SyncOperation.CREATE, 2L,
        // directory1
        SyncOperation.DELETE, 1L,
        // file1
        SyncOperation.RECREATE, 1L
    ));
  }

  @Test
  public void syncDirectoryTestUFSIteration() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    for (int i = 0; i < 100; ++i) {
      mS3Client.putObject(TEST_BUCKET, "file" + i, "");
    }

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 100L
    ));
  }

  @Test
  public void syncDirectoryTestUFSIterationRecursive() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    int filePerDirectory = 5;
    // count the files
    int createdInodeCount = filePerDirectory * filePerDirectory * filePerDirectory;
    // count the directories
    createdInodeCount += filePerDirectory * filePerDirectory + filePerDirectory;

    for (int i = 0; i < filePerDirectory; ++i) {
      for (int j = 0; j < filePerDirectory; ++j) {
        for (int k = 0; k < filePerDirectory; ++k) {
          mS3Client.putObject(TEST_BUCKET, String.format("%d/%d/%d", i, j, k), "");
        }
      }
    }

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    assertTrue(result.succeeded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, (long) createdInodeCount
    ));

    // count the files
    int noopInodeCount = filePerDirectory * filePerDirectory * filePerDirectory;
    if (mDirectoryLoadType != DirectoryLoadType.SINGLE_LISTING) {
      // count the directories
      noopInodeCount += filePerDirectory * filePerDirectory + filePerDirectory;
    }

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    // All created node were not changed.
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.NOOP, (long) noopInodeCount
    ));
  }

  @Test
  public void syncNonS3DirectoryDelete()
      throws Throwable {
    // Create a directory not on local ufs
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory"),
        CreateDirectoryContext.defaults());
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory/sub_directory"),
        CreateDirectoryContext.defaults());
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_directory"), DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 2L
    ));

    // Create a directory not on local ufs
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory"),
        CreateDirectoryContext.defaults());
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory/sub_directory"),
        CreateDirectoryContext.defaults());
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_directory"), DescendantType.ONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 2L
    ));

    // Create a directory not on local ufs
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory"),
        CreateDirectoryContext.defaults());
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory/sub_directory"),
        CreateDirectoryContext.defaults());
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_directory"), DescendantType.NONE, mDirectoryLoadType, 0)
        .getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.DELETE, 2L
    ));
  }

//  @Test
//  public void syncNonS3Directory()
//      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
//      IOException, InvalidPathException {
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    // Create a directory not on local ufs
//    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory"),
//        CreateDirectoryContext.defaults());
//    SyncResult result =
//        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"),
//            createContext(DescendantType.ONE));
//    assertTrue(result.getSuccess());
//    assertSyncOperations(result, ImmutableMap.of(
//        SyncOperation.NOOP, 1L,
//        SyncOperation.DELETE, 1L,
//        SyncOperation.SKIPPED_ON_MOUNT_POINT, 0L
//    ));
//  }
//

  @Test
  public void testS3Fingerprint() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "f3", TEST_CONTENT);

    // Sync to load metadata

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);

    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 3L
    ));

    mS3Client.putObject(TEST_BUCKET, "f1", "");
    mS3Client.putObject(TEST_BUCKET, "f2", TEST_CONTENT);

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    long mountPointInodeId = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync()).getFileId();
    assertTrue(mFileSystemMaster.getInodeStore()
        .get(mountPointInodeId).get().asDirectory().isDirectChildrenLoaded());
    checkUfsMatches(MOUNT_POINT, TEST_BUCKET, "", mFileSystemMaster, mClient);
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        // f1, f3
        SyncOperation.NOOP, 2L,
        // f2
        SyncOperation.RECREATE, 1L
    ));
  }

  @Test
  public void syncNoneOnMountPoint1() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "d1/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d1/f2", TEST_CONTENT);

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.NONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
    ));
  }

  @Test
  public void syncNoneOnMountPoint2() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "d1/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d1/f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d2/f1", TEST_CONTENT);

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.NONE, mDirectoryLoadType, 0).getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
    ));
  }

  @Test
  public void syncUfsNotFound() throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT.join("/non_existing_path"), DescendantType.ALL, mDirectoryLoadType, 0)
        .getBaseTask();
    result.waitComplete(TIMEOUT_MS);
    assertTrue(mFileSystemMaster.getAbsentPathCache().isAbsentSince(
        new AlluxioURI("/non_existing_path"), 0));
  }

//
//
//  // TODO(elega) -> this is not correct
//  // Two options to deal with unmount-during-sync
//  // Option 1: add read lock on the sync path
//  // Option 2: cancel the ongoing metadata sync job
//  @Test
//  public void unmountDuringSync() throws Exception {
//    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();
//
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    for (int i = 0; i < 100; ++i) {
//      mS3Client.putObject(TEST_BUCKET, "file" + i, "");
//    }
//
//    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
//      try {
//        return mFileSystemMaster.syncMetadataInternal(
//            MOUNT_POINT, createContextWithBatchSize(DescendantType.ONE, 10));
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    });
//    syncer.blockUntilNthSyncThenDo(50, () -> mFileSystemMaster.unmount(MOUNT_POINT));
//    SyncResult result = syncFuture.get();
//    // This is not expected
//    assertTrue(mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(true)).size() < 100);
//  }
//
//  @Test
//  public void concurrentDelete() throws Exception {
//    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();
//
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    // Create a directory not on s3 ufs
//    mFileSystemMaster.createDirectory(MOUNT_POINT.join("/d"),
//        CreateDirectoryContext.defaults().setWriteType(WriteType.MUST_CACHE));
//    // Create something else into s3
//    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
//
//    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
//      try {
//        return mFileSystemMaster.syncMetadataInternal(MOUNT_POINT,
//            createContext(DescendantType.ALL));
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    });
//    // blocks on the sync of "/d" (the 2nd sync target)
//    syncer.blockUntilNthSyncThenDo(2,
//        () -> mFileSystemMaster.delete(MOUNT_POINT.join("/d"), DeleteContext.defaults()));
//    SyncResult result = syncFuture.get();
//    assertTrue(result.getSuccess());
//    assertSyncOperations(result, ImmutableMap.of(
//        // root
//        SyncOperation.NOOP, 1L,
//        // d
//        SyncOperation.SKIPPED_DUE_TO_CONCURRENT_MODIFICATION, 1L,
//        // test-file
//        SyncOperation.CREATE, 1L
//    ));
//  }
//
//  @Test
//  public void concurrentCreate() throws Exception {
//    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();
//
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
//
//    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
//      try {
//        return mFileSystemMaster.syncMetadataInternal(MOUNT_POINT,
//            createContext(DescendantType.ALL));
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    });
//    // blocks on the sync of "/test_file" (the 2nd sync target)
//    syncer.blockUntilNthSyncThenDo(2,
//        () -> mFileSystemMaster.createFile(MOUNT_POINT.join(TEST_FILE),
//            CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE)));
//    SyncResult result = syncFuture.get();
//    assertTrue(result.getSuccess());
//    assertSyncOperations(result, ImmutableMap.of(
//        // root
//        SyncOperation.NOOP, 1L,
//        // test-file
//        SyncOperation.SKIPPED_DUE_TO_CONCURRENT_MODIFICATION, 1L
//    ));
//  }
//
//  @Test
//  public void concurrentUpdateRoot() throws Exception {
//    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();
//
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
//    mFileSystemMaster.createFile(MOUNT_POINT.join(TEST_FILE),
//        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));
//
//    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
//      try {
//        return mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE),
//            createContext(DescendantType.NONE));
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    });
//    syncer.blockUntilNthSyncThenDo(1,
//        () -> mFileSystemMaster.delete(MOUNT_POINT.join(TEST_FILE), DeleteContext.defaults()));
//    SyncResult result = syncFuture.get();
//    assertFalse(result.getSuccess());
//    assertEquals(SyncFailReason.CONCURRENT_UPDATE_DURING_SYNC, result.getFailReason());
//  }
//
//  private MetadataSyncContext createContext(DescendantType descendantType)
//      throws UnavailableException {
//    return MetadataSyncContext.Builder.builder(
//        mFileSystemMaster.createRpcContext(), descendantType).build();
//  }
//
//  private MetadataSyncContext createContextWithBatchSize(
//      DescendantType descendantType, int batchSize) throws UnavailableException {
//    return MetadataSyncContext.Builder.builder(
//        mFileSystemMaster.createRpcContext(), descendantType).setBatchSize(batchSize).build();
//  }
//
//  @Test
//  public void startAfter() throws Exception {
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    mS3Client.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "f2", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "f3", TEST_CONTENT);
//    // The S3 mock server has a bug where 403 is returned if startAfter exceeds the last
//    // object key.
//    MetadataSyncContext context =
//        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                DescendantType.ALL)
//        .setStartAfter("f2").build();
//    SyncResult result =
//        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, context);
//    assertTrue(result.getSuccess());
//    assertEquals(1, mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(false)).size());
//
//    context =
//        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                DescendantType.ALL)
//            .setStartAfter("f1").build();
//    result =
//        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, context);
//    assertTrue(result.getSuccess());
//    assertEquals(2, mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(false)).size());
//
//    context =
//        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                DescendantType.ALL)
//            .setStartAfter("a").build();
//    result =
//        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, context);
//    assertTrue(result.getSuccess());
//    assertEquals(3, mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(false)).size());
//  }
//
//  @Test
//  public void startAfterAbsolutePath() throws Exception {
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    mS3Client.putObject(TEST_BUCKET, "root/f1", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/f2", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/f3", TEST_CONTENT);
//    // The S3 mock server has a bug where 403 is returned if startAfter exceeds the last
//    // object key.
//    assertThrows(InvalidPathException.class, () -> {
//      MetadataSyncContext context =
//          MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                  DescendantType.ONE)
//              .setStartAfter("/random/path").build();
//      SyncResult result =
//          mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
//    });
//
//    MetadataSyncContext context =
//        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                DescendantType.ONE)
//            .setStartAfter("/s3_mount/root/f2").build();
//    SyncResult result =
//        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
//    assertTrue(result.getSuccess());
//    assertEquals(1, mFileSystemMaster.listStatus(MOUNT_POINT.join("root"),
//        listNoSync(false)).size());
//
//    context =
//        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                DescendantType.ONE)
//            .setStartAfter("/s3_mount/root").build();
//    result =
//        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
//    assertTrue(result.getSuccess());
//    assertEquals(3, mFileSystemMaster.listStatus(MOUNT_POINT.join("root"),
//        listNoSync(false)).size());
//    // TODO(elega) look into WARNING: xattr not supported on root/
//  }
//
//  @Test
//  public void startAfterRecursive() throws Exception {
//    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
//    mS3Client.putObject(TEST_BUCKET, "root/d1/d1/f1", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/d1/d1/f2", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/d1/d2/f1", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/d1/d2/f3", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/d1/f1", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/d2/f1", TEST_CONTENT);
//    mS3Client.putObject(TEST_BUCKET, "root/f1", TEST_CONTENT);
//    // The S3 mock server has a bug where 403 is returned if startAfter exceeds the last
//    // object key.
//    MetadataSyncContext context =
//        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
//                DescendantType.ALL)
//            .setStartAfter("d1/d2/f2").build();
//    SyncResult result =
//        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
//    // Files are created recursively so the # of file created in the result is less than
//    // the actual # of files created. Checking the alluxio inode tree instead.
//    assertTrue(result.getSuccess());
//    /*
//    (under "/s3_mount/root")
//      /d1
//        /d2
//          /f3
//        /f1
//      /d2
//        /d1
//      /f1
//     */
//    assertEquals(7,
//        mFileSystemMaster.listStatus(MOUNT_POINT.join("root"), listNoSync(true)).size());
//  }
//
}
