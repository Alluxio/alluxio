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

package alluxio.client.rest;

import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response.Status;

public class CreateBucketTest extends RestApiTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final int UFS_PORT = 8002;
  private AmazonS3 mS3Client = null;
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(UFS_PORT)
      .withCredentials("_", "_")
      .build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeProxy(true)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:" + UFS_PORT)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
          .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.WORKER_HTTP_SERVER_ENABLED, false)
          .setProperty(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey())
          .setProperty(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey())
          .setNumWorkers(2)
          .build();

  @Before
  public void before() throws Exception {
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
    mHostname = mLocalAlluxioClusterResource.get().getHostname();
    mPort = mLocalAlluxioClusterResource.get().getProxyProcess().getWebLocalPort();
    mBaseUri = String.format("/api/v1/s3");
  }

  @After
  public void after() {
    mS3Client = null;
  }

  /**
   * Creates a bucket. Creates an existent bucket.
   */
  @Test
  public void createBucket() throws Exception {
    String bucketName = "bucket";
    // Heads a non-existent bucket.
    headTestCase(bucketName).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    // Creates a bucket.
    createBucketTestCase(bucketName).checkResponseCode(Status.OK.getStatusCode());
    // Heads a bucket.
    headTestCase(bucketName).checkResponseCode(Status.OK.getStatusCode());
    // Creates an existent bucket.
    createBucketTestCase(bucketName).checkResponseCode(Status.CONFLICT.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.BUCKET_ALREADY_EXISTS);
  }

  /**
   * Deletes a non-existent bucket. Deletes an existent bucket.
   */
  @Test
  public void deleteBucket() throws Exception {
    String bucketName = "bucket";
    headTestCase(bucketName).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    // Deletes a non-existent bucket.
    deleteTestCase(bucketName).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
    createBucketTestCase(bucketName).checkResponseCode(Status.OK.getStatusCode());
    // Deletes an existent bucket.
    deleteTestCase(bucketName).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(bucketName).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }
}
