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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.ExistsContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.metasync.SyncFailReason;
import alluxio.master.file.metasync.SyncOperation;
import alluxio.master.file.metasync.SyncResult;
import alluxio.wire.FileInfo;

import com.adobe.testing.s3mock.junit4.S3MockRule;
//import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.adobe.testing.s3mock.testsupport.common.S3MockStarter;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
public final class FileSystemMetadataSyncV2Test extends FileSystemMasterTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMetadataSyncV2Test.class);
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_BUCKET2 = "test-bucket-2";
  private static final String TEST_FILE = "test_file";
  private static final String TEST_DIRECTORY = "test_directory";
  private static final String TEST_CONTENT = "test_content";
  private static final AlluxioURI UFS_ROOT = new AlluxioURI("s3://test-bucket/");
  private static final AlluxioURI UFS_ROOT2 = new AlluxioURI("s3://test-bucket2/");
  private static final AlluxioURI MOUNT_POINT = new AlluxioURI("/s3_mount");
  private static final AlluxioURI MOUNT_POINT2 = new AlluxioURI("/s3_mount2");
  private static final AlluxioURI NESTED_MOUNT_POINT = new AlluxioURI("/mnt/nested_s3_mount");
  private static final AlluxioURI NESTED_S3_MOUNT_POINT = new AlluxioURI("/s3_mount/nested_s3_mount");
  private AmazonS3 mS3Client;
  // disable S3 mock server and use adobe s3mock for now
  // private S3Mock mS3MockServer;

  @Rule
  public final S3MockRule s3MockRule = S3MockRule.builder().silent().withHttpPort(8001).build();

  // Invalid keystore format
  // install the latest jdk 8
  @Override
  public void before() throws Exception {
    /*
    mS3MockServer = new S3Mock.Builder().withPort(8002).withInMemoryBackend().build();
    mS3MockServer.start();
     */

    Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001");
    Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2");
    Configuration.set(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true);
    Configuration.set(PropertyKey.S3A_ACCESS_KEY, "_");
    Configuration.set(PropertyKey.S3A_SECRET_KEY, "_'");

    AwsClientBuilder.EndpointConfiguration
        endpoint = new AwsClientBuilder.EndpointConfiguration(
        "http://localhost:8001", "us-west-2");
    mS3Client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(endpoint)
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();
    mS3Client.createBucket(TEST_BUCKET);
    mS3Client.createBucket(TEST_BUCKET2);
    super.before();
  }

  @Test
  public void syncInodeHappyPath()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    // Sync one file from UFS
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), DescendantType.ONE);
    assertTrue(result.getSuccess());
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.CREATE, 0L));
    FileInfo info = mFileSystemMaster.getFileInfo(MOUNT_POINT.join(TEST_FILE), getNoSync());
    assertFalse(info.isFolder());
    assertTrue(info.isCompleted());

    // Sync again, expect no change
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), DescendantType.ONE);
    assertTrue(result.getSuccess());
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.NOOP, 0L));

    // Delete the file from UFS, then sync again
    mS3Client.deleteObject(TEST_BUCKET, TEST_FILE);
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), DescendantType.NONE);
    assertTrue(result.getSuccess());
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.RECREATE, 0L));
    info = mFileSystemMaster.getFileInfo(MOUNT_POINT.join(TEST_FILE), getNoSync());
    assertTrue(info.isFolder());
  }

  @Test
  public void syncInodeUfsDown()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException, NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
    Method m = S3MockStarter.class.getDeclaredMethod("stop");
    m.setAccessible(true);
    m.invoke(s3MockRule);

    stopS3Server();
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), DescendantType.NONE);
    assertFalse(result.getSuccess());
    assertEquals(SyncFailReason.UFS_IO_FAILURE, result.getFailReason());
    startS3Server();
  }

  @Test
  public void syncDirectoryHappyPath()
      throws FileDoesNotExistException, InvalidFileSizeException, IOException,
      BlockInfoException, AccessControlException, FileAlreadyCompletedException,
      InvalidPathException, FileAlreadyExistsException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "file1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "file2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "file3", TEST_CONTENT);

    // To recreate -> content hashes are different
    mFileSystemMaster.createFile(MOUNT_POINT.join("file1"), CreateFileContext.defaults());
    mFileSystemMaster.completeFile(MOUNT_POINT.join("file1"), CompleteFileContext.defaults());

    // To delete -> doesn't exist in UFS
    mFileSystemMaster.createDirectory(MOUNT_POINT.join("directory1"), CreateDirectoryContext.defaults());

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, DescendantType.ONE);
    assertTrue(result.getSuccess());

    // file2 & file3
    assertEquals(2, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.CREATE, 0L));
    // directory1
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.DELETE, 0L));
    // file1
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.RECREATE, 0L));
    // sync root
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.NOOP, 0L));
  }

  @Test
  public void syncDirectoryTestUFSIteration()
      throws FileDoesNotExistException, InvalidFileSizeException, IOException,
      BlockInfoException, AccessControlException, FileAlreadyCompletedException,
      InvalidPathException, FileAlreadyExistsException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    for (int i = 0; i < 100; ++i) {
      mS3Client.putObject(TEST_BUCKET, "file" + i, "");
    }

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, DescendantType.ONE, 10);
    assertTrue(result.getSuccess());
    assertEquals(100, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.CREATE, 0L));
  }

  @Test
  public void syncDirectoryTestUFSIterationRecursive()
      throws FileDoesNotExistException, InvalidFileSizeException, IOException,
      BlockInfoException, AccessControlException, FileAlreadyCompletedException,
      InvalidPathException, FileAlreadyExistsException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    int filePerDirectory = 5;
    int createdInodeCount = filePerDirectory * filePerDirectory * filePerDirectory +
        filePerDirectory * filePerDirectory + filePerDirectory;
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 5; ++j) {
        for (int k = 0; k < 5; ++k) {
          mS3Client.putObject(TEST_BUCKET, String.format("%d/%d/%d", i, j, k), "");
        }
      }
    }

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, DescendantType.ALL, 10);
    assertTrue(result.getSuccess());
    assertEquals(createdInodeCount, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.CREATE, 0L));

    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, DescendantType.ALL, 10);
    assertTrue(result.getSuccess());
    // All created node + root were not changed.
    assertEquals(createdInodeCount + 1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.NOOP, 0L));
  }

  @Test
  public void syncNonS3Directory()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    // Create a directory not on local ufs
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory"), CreateDirectoryContext.defaults());
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"), DescendantType.ONE);
    assertTrue(result.getSuccess());
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.DELETE, 0L));
    assertEquals(1, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.SKIPPED_ON_MOUNT_POINT, 0L));
  }

  @Test(expected = InvalidPathException.class)
  public void syncS3DirectoryNestedMount()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mFileSystemMaster.mount(NESTED_S3_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    // In the existing UFS S3 implementation, ufs.exists() always returns true,
    // regardless if an object exists in s3 or not. If the object does not exist,
    // alluxio S3 UFS implementation treats it as a pseudo directory.
    // This essentially makes it impossible to do a nested mount under an s3 mount point.
  }


  // TODO (this test still has some issues)
  @Test
  public void syncNonS3RecursiveUnsupported()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    // mount /s3_mount -> s3://test-bucket
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "foo/bar", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "foo/baz", TEST_CONTENT);

    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.THROUGH));
    // mount /mnt/nested_s3_mount -> s3://test-bucket2
    mFileSystemMaster.mount(NESTED_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET2, "foo/bar", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET2, "foo/baz", TEST_CONTENT);

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"), DescendantType.ALL);
    List<FileInfo> inodes = mFileSystemMaster.listStatus(new AlluxioURI("/"), listNoSync(true));
    assertFalse(result.getSuccess());
    assertEquals(SyncFailReason.UNSUPPORTED, result.getFailReason());
  }


  private ListStatusContext listNoSync(boolean isRecursive) {
    return ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setRecursive(isRecursive)
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  private GetStatusContext getNoSync() {
    return GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  private ExistsContext existsNoSync() {
    return ExistsContext.mergeFrom(ExistsPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }


  private void stopS3Server() {
    try {
      Method method = S3MockStarter.class.getDeclaredMethod("stop");
      method.setAccessible(true);
      method.invoke(s3MockRule);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startS3Server() {
    try {
      Method method = S3MockStarter.class.getDeclaredMethod("start");
      method.setAccessible(true);
      method.invoke(s3MockRule);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void after() throws Exception {
    mS3Client = null;
    /*
    try {
      if (mS3MockServer != null) {
        mS3MockServer.shutdown();
      }
    } finally {
      mS3MockServer = null;
    }
     */
    super.after();
  }
}
