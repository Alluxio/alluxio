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

import static alluxio.master.file.MetadataSyncV2TestBase.TIMEOUT_MS;
import static alluxio.master.file.MetadataSyncV2TestBase.assertSyncOperations;
import static alluxio.master.file.MetadataSyncV2TestBase.existsNoSync;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.master.file.metasync.SyncOperation;
import alluxio.master.mdsync.BaseTask;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MetadataSyncNonObjectStoreV2Test extends FileSystemMasterTestBase {

  DirectoryLoadType mDirectoryLoadType;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {DirectoryLoadType.SINGLE_LISTING},
        {DirectoryLoadType.BFS},
        {DirectoryLoadType.DFS},
    });
  }

  public MetadataSyncNonObjectStoreV2Test(DirectoryLoadType directoryLoadType) {
    mDirectoryLoadType = directoryLoadType;
  }

  @Test
  public void syncNonS3DirectorySync()
      throws Throwable {
    String path = mFileSystemMaster.getMountTable().resolve(new AlluxioURI("/")).getUri().getPath();
    assertTrue(new File(path + "/test_file").createNewFile());
    assertTrue(new File(path + "/test_directory").mkdir());
    assertTrue(new File(path + "/test_directory/test_file").createNewFile());
    assertTrue(new File(path + "/test_directory/nested_directory").mkdir());
    assertTrue(new File(path + "/test_directory/nested_directory/test_file").createNewFile());

    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_directory"), DescendantType.NONE, mDirectoryLoadType, 0);
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));
    assertTrue(mFileSystemMaster.exists(new AlluxioURI("/test_directory"), existsNoSync()));

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_file"), DescendantType.NONE, mDirectoryLoadType, 0);
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));
    assertTrue(mFileSystemMaster.exists(new AlluxioURI("/test_file"), existsNoSync()));

    // TODO(yimin) when the descendant type is ONE/ALL, seems like the NOOP of the root inode
    // itself is not counted.
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_directory"), DescendantType.ONE, mDirectoryLoadType, 0);
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 2L,
        SyncOperation.NOOP, 1L
    ));
    assertTrue(mFileSystemMaster.exists(new AlluxioURI("/test_directory"), existsNoSync()));

    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        new AlluxioURI("/test_directory"), DescendantType.ALL, mDirectoryLoadType, 0);
    result.waitComplete(TIMEOUT_MS);
    assertTrue(result.succeeded());
    assertSyncOperations(result.getTaskInfo(), ImmutableMap.of(
        SyncOperation.CREATE, 1L,
        SyncOperation.NOOP, 3L
    ));
    assertTrue(mFileSystemMaster.exists(new AlluxioURI("/test_directory"), existsNoSync()));
  }

  //  @Test
//  public void testNonS3Fingerprint() throws Exception {
//    // this essentially creates a directory and mode its alluxio directory without
//    // syncing the change down to ufs
//    mFileSystemMaster.createDirectory(new AlluxioURI("/d"),
//        CreateDirectoryContext.defaults().setWriteType(WriteType.THROUGH));
//    mFileSystemMaster.delete(new AlluxioURI("/d"),
//        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setAlluxioOnly(true)));
//    mFileSystemMaster.createDirectory(new AlluxioURI("/d"),
//        CreateDirectoryContext.mergeFrom(
//                CreateDirectoryPOptions.newBuilder().setMode(new Mode((short) 0777).toProto()))
//            .setWriteType(WriteType.MUST_CACHE));
//
//    SyncResult result =
//        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"),
//            createContext(DescendantType.ONE));
//
//    assertSyncOperations(result, ImmutableMap.of(
//        // root
//        SyncOperation.NOOP, 1L,
//        // d
//        SyncOperation.UPDATE, 1L
//    ));
//  }
}
