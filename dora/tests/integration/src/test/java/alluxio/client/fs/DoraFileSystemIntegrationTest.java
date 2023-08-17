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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.journal.JournalType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.io.IOUtils;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Integration tests for Alluxio Client (reuse the {@link LocalAlluxioCluster}).
 */
public final class DoraFileSystemIntegrationTest extends BaseIntegrationTest {
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  LocalAlluxioClusterResource.Builder mLocalAlluxioClusterResourceBuilder =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, Constants.MB * 16)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Long.MAX_VALUE)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, Constants.MB * 16 / 2)
          .setProperty(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, true)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
          .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, "1GB")
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.NOOP)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001")
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
          .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.WORKER_HTTP_SERVER_ENABLED, false)
          .setProperty(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey())
          .setProperty(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey())
          .setNumWorkers(2)
          .setStartCluster(false);

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_FILE = "test-file";
  private static final AlluxioURI TEST_FILE_URI = new AlluxioURI("/" + "test-file");
  private static final String TEST_CONTENT = "test-content";
  private static final String UPDATED_TEST_CONTENT = "updated-test-content";

  private FileSystem mFileSystem = null;
  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  private AmazonS3 mS3Client = null;

  @Before
  public void before() throws Exception {
  }

  private void startCluster(LocalAlluxioClusterResource cluster) throws Exception
  {
    cluster.start();
    mFileSystem = cluster.get().getClient();

    if (mS3Client == null) {
      mS3Client = AmazonS3ClientBuilder
          .standard()
          .withPathStyleAccessEnabled(true)
          .withCredentials(
              new AWSStaticCredentialsProvider(
                  new BasicAWSCredentials(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(mS3Proxy.getUri().toString(),
                  Regions.US_WEST_2.getName()))
          .build();
      mS3Client.createBucket(TEST_BUCKET);
    }
  }

  private void stopCluster(LocalAlluxioClusterResource cluster) throws Exception
  {
    mFileSystem = null;
    cluster.stop();
  }

  /**
   * Writes a file through alluxio into UFS. Deletes the file from UFS.
   * Read the file with sync interval setting to -1 should give the cached file.
   * Read the file with sync interval setting to 0 should return error.
   */
  private void writeThenDeleteFromUfs(boolean clientWriteToUFS)
      throws IOException, AlluxioException, Exception {
    mLocalAlluxioClusterResourceBuilder.setProperty(PropertyKey.CLIENT_WRITE_TO_UFS_ENABLED,
                                                    clientWriteToUFS);
    LocalAlluxioClusterResource clusterResource = mLocalAlluxioClusterResourceBuilder.build();
    startCluster(clusterResource);

    FileOutStream fos = mFileSystem.createFile(TEST_FILE_URI,
        CreateFilePOptions.newBuilder().setOverwrite(true).build());
    fos.write(TEST_CONTENT.getBytes());
    fos.close();

    mS3Client.deleteObject(TEST_BUCKET, TEST_FILE);
    assertNotNull(mFileSystem.getStatus(TEST_FILE_URI, GetStatusPOptions.newBuilder()
        .setCommonOptions(optionNoSync())
        .build()));
    try (FileInStream fis = mFileSystem.openFile(TEST_FILE_URI,
        OpenFilePOptions.newBuilder().setCommonOptions(optionNoSync()).build())) {
      String content = IOUtils.toString(fis);
      assertEquals(TEST_CONTENT, content);
    }

    assertThrows(FileDoesNotExistException.class, () ->
        mFileSystem.getStatus(TEST_FILE_URI, GetStatusPOptions.newBuilder()
            .setCommonOptions(optionSync()).build()));

    assertThrows(FileDoesNotExistException.class, () ->
        mFileSystem.getStatus(TEST_FILE_URI, GetStatusPOptions.newBuilder()
            .setCommonOptions(optionNoSync()).build()));

    stopCluster(clusterResource);
  }

  /**
   * Writes a file through alluxio into UFS, then updates the file from UFS.
   * Read the file with sync interval setting to -1 should give the cached file.
   * Read the file with sync interval setting to 0 should return the updated file content.
   */
  private void writeThenUpdateFromUfs(boolean clientWriteToUFS)
      throws IOException, AlluxioException, Exception {
    mLocalAlluxioClusterResourceBuilder.setProperty(PropertyKey.CLIENT_WRITE_TO_UFS_ENABLED,
                                                    clientWriteToUFS);
    LocalAlluxioClusterResource clusterResource = mLocalAlluxioClusterResourceBuilder.build();
    startCluster(clusterResource);

    FileOutStream fos = mFileSystem.createFile(TEST_FILE_URI,
        CreateFilePOptions.newBuilder().setOverwrite(true).build());
    fos.write(TEST_CONTENT.getBytes());
    fos.close();

    mS3Client.putObject(TEST_BUCKET, TEST_FILE, UPDATED_TEST_CONTENT);
    assertNotNull(mFileSystem.getStatus(TEST_FILE_URI, GetStatusPOptions.newBuilder()
        .setCommonOptions(optionNoSync())
        .build()));

    try (FileInStream fis = mFileSystem.openFile(TEST_FILE_URI,
        OpenFilePOptions.newBuilder().setCommonOptions(optionNoSync()).build())) {
      String content = IOUtils.toString(fis);
      assertEquals(TEST_CONTENT, content);
    }

    // This will update/sync metadata from UFS
    mFileSystem.getStatus(TEST_FILE_URI, GetStatusPOptions.newBuilder()
        .setCommonOptions(optionSync()).build());

    // metadata is already updated. Even though we are not going to sync metadata in Read(),
    // it should get the latest content.
    try (FileInStream fis = mFileSystem.openFile(TEST_FILE_URI,
        OpenFilePOptions.newBuilder().setCommonOptions(optionNoSync()).build())) {
      String content = IOUtils.toString(fis);
      assertEquals(UPDATED_TEST_CONTENT, content);
    }

    stopCluster(clusterResource);
  }

  private FileSystemMasterCommonPOptions optionNoSync() {
    return FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            .build();
  }

  private FileSystemMasterCommonPOptions optionSync() {
    return FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)
            .build();
  }

  /**
   * Writes a file through alluxio into UFS. Deletes the file from UFS.
   * Read the file with sync interval setting to -1 should give the cached file.
   * Read the file with sync interval setting to 0 should return error.
   */
  @Test
  public void testWriteThenDeleteFromUfs() throws Exception {
    writeThenDeleteFromUfs(true);
    writeThenDeleteFromUfs(false);
  }

  /**
   * Writes a file through alluxio into UFS, then updates the file from UFS.
   * Read the file with sync interval setting to -1 should give the cached file.
   * Read the file with sync interval setting to 0 should return the updated file content.
   */
  @Test
  public void testWriteThenUpdateFromUfs() throws Exception {
    writeThenUpdateFromUfs(true);
    writeThenUpdateFromUfs(false);
  }
}
