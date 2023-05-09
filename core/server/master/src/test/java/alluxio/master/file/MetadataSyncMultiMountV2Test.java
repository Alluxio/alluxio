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
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.mdsync.SyncOperation;
import alluxio.master.file.mdsync.TaskGroup;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class MetadataSyncMultiMountV2Test extends MetadataSyncV2TestBase {
  public MetadataSyncMultiMountV2Test(DirectoryLoadType directoryLoadType) {
    mDirectoryLoadType = directoryLoadType;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {DirectoryLoadType.SINGLE_LISTING},
        {DirectoryLoadType.BFS},
        {DirectoryLoadType.DFS},
    });
  }

  @Test
  public void syncNonS3DirectoryShadowingMountPoint()
      throws Throwable {
    /*
      / (root) -> local file system (disk)
      /s3_mount -> s3 bucket
      create /s3_mount in the local first system that shadows the mount point and then do
      a metadata sync on root
      the sync of the local file system /s3_mount is expected to be skipped
     */

    String localUfsPath
        = mFileSystemMaster.getMountTable().resolve(MOUNT_POINT).getUri().getPath();
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    assertTrue(new File(localUfsPath).createNewFile());
    TaskGroup result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/"), DescendantType.ALL, mDirectoryLoadType, 0);
    result.waitAllComplete(TIMEOUT_MS);
    assertTrue(result.allSucceeded());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.SKIPPED_ON_MOUNT_POINT, 1L
    ));
    FileInfo mountPointFileInfo = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync());
    assertTrue(mountPointFileInfo.isMountPoint());
    assertTrue(mountPointFileInfo.isFolder());
  }

  @Test
  public void syncNestedS3Mount()
      throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mFileSystemMaster.mount(NESTED_S3_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "d/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET2, "f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET2, "d/f2", TEST_CONTENT);

    /*
      / (ROOT) -> unchanged (root mount point local fs)
        /s3_mount -> unchanged (mount point s3://test-bucket)
          /f1 -> created
          /d -> pseudo directory (created)
            /f1 -> (created)
          /nested_s3_mount -> unchanged (mount point s3://test-bucket-2)
            /f2 -> created
            /d -> pseudo directory (created)
              /f2 -> (created)
     */
    TaskGroup result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/"), DescendantType.ALL, mDirectoryLoadType, 0);
    result.waitAllComplete(TIMEOUT_MS);
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.CREATE, 6L
    ));
    assertTrue(result.allSucceeded());

    List<FileInfo> inodes = mFileSystemMaster.listStatus(new AlluxioURI("/"), listNoSync(true));
    assertEquals(8, inodes.size());
    assertTrue(mFileSystemMaster.exists(NESTED_S3_MOUNT_POINT.join("d/f2"), existsNoSync()));
    assertTrue(mFileSystemMaster.exists(MOUNT_POINT.join("d/f1"), existsNoSync()));
  }

  @Test
  public void syncNestedS3MountShadowingMountPoint()
      throws Throwable {
    /*
      / (ROOT) -> unchanged (root mount point local fs)
        /s3_mount -> unchanged (mount point s3://test-bucket)
          /nested_s3_mount -> unchanged (mount point s3://test-bucket-2)
            /foo -> created
          /nested_s3_mount -> SHADOWED (mount point s3://test-bucket)
            /shadowed -> SHADOWED
            /bar/baz -> SHADOWED
        /not_shadowed -> created
     */

    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mFileSystemMaster.mount(NESTED_S3_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "nested_s3_mount/shadowed", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "nested_s3_mount/bar/baz", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "not_shadowed", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET2, "foo", TEST_CONTENT);

    TaskGroup result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/"), DescendantType.ALL, mDirectoryLoadType, 0);
    result.waitAllComplete(TIMEOUT_MS);
    result.getTasks()
        .forEach(it -> System.out.println(it.getTaskInfo().getStats().toReportString()));
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.CREATE, 2L,
        SyncOperation.SKIPPED_ON_MOUNT_POINT, mDirectoryLoadType
            == DirectoryLoadType.SINGLE_LISTING ? 2L : 1L
    ));
    assertTrue(result.allSucceeded());
    List<FileInfo> inodes = mFileSystemMaster.listStatus(new AlluxioURI("/"), listNoSync(true));
    assertEquals(4, inodes.size());
  }

  @Test
  public void syncS3NestedMountLocalFs()
      throws Throwable {
    // mount /s3_mount -> s3://test-bucket
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "foo/bar", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "foo/baz", TEST_CONTENT);

    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.THROUGH));
    // mount /mnt/nested_s3_mount -> s3://test-bucket-2
    mFileSystemMaster.mount(NESTED_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET2, "foo/bar", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET2, "foo/baz", TEST_CONTENT);

    TaskGroup result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/"), DescendantType.ONE, mDirectoryLoadType, 0);
    result.waitAllComplete(TIMEOUT_MS);
    assertTrue(result.allSucceeded());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));
    assertEquals(1, result.getTaskCount());

    /*
      / (ROOT) -> unchanged (root mount point local fs)
        /s3_mount -> unchanged (mount point s3://test-bucket)
          /foo -> pseudo directory (created)
            /bar -> (created)
            /baz -> (created)
        /mnt -> unchanged
          /nested_s3_mount -> unchanged (mount point s3://test-bucket-2)
            /foo -> pseudo directory (created)
              /bar -> (created)
              /baz -> (created)
     */
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/"), DescendantType.ALL, mDirectoryLoadType, 0);
    result.waitAllComplete(TIMEOUT_MS);
    assertTrue(result.allSucceeded());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, 1L,
        SyncOperation.CREATE, 6L
    ));
    assertEquals(3, result.getTaskCount());

    List<FileInfo> inodes = mFileSystemMaster.listStatus(new AlluxioURI("/"), listNoSync(true));
    assertEquals(9, inodes.size());
  }
}
