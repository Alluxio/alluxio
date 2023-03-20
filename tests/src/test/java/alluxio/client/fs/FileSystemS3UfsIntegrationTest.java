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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileSystemS3UfsIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_CONTENT = "TestContents";
  private static final String TEST_FILE = "test_file";
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001")
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
          .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.S3A_ACCESS_KEY, "_")
          .setProperty(PropertyKey.S3A_SECRET_KEY, "_")
          .setStartCluster(false)
          .build();
  private FileSystem mFileSystem = null;
  private AmazonS3 mS3Client = null;
  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  private S3Mock mS3MockServer;
  private static final String TEST_BUCKET = "test-bucket";

  @Before
  public void before() throws Exception {
    mS3MockServer = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    mS3MockServer.start();
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

    mLocalAlluxioClusterResource.start();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() {
    mS3Client = null;
    try {
      if (mS3MockServer != null) {
        mS3MockServer.shutdown();
      }
    } finally {
      mS3MockServer = null;
    }
  }

  @Test
  public void basicMetadataSync() throws IOException, AlluxioException {
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
    FileInStream fis = mFileSystem.openFile(new AlluxioURI("/" + TEST_FILE));
    assertEquals(TEST_CONTENT, IOUtils.toString(fis, StandardCharsets.UTF_8));
  }

  @Test
  public void basicWriteThrough() throws IOException, AlluxioException {
    FileOutStream fos = mFileSystem.createFile(
        new AlluxioURI("/" + TEST_FILE),
        CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build());
    fos.write(TEST_CONTENT.getBytes());
    fos.close();
    try (S3Object s3Object = mS3Client.getObject(TEST_BUCKET, TEST_FILE)) {
      assertEquals(
          TEST_CONTENT, IOUtils.toString(s3Object.getObjectContent(), StandardCharsets.UTF_8));
    }
  }
}
