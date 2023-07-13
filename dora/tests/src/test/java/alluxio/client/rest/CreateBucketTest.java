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
import alluxio.master.journal.JournalType;
import alluxio.proxy.s3.S3Error;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.HttpURLConnection;
import javax.ws.rs.core.Response;

public class CreateBucketTest extends RestApiTest {

  private static final String TEST_BUCKET = "test-bucket";
  private AmazonS3 mS3Client = null;
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeProxy(true)
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
          .setStartCluster(false)
          .build();

  @Before
  public void before() throws Exception {
    mLocalAlluxioClusterResource.start();

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
   * Creates a bucket.
   */
  @Test
  public void createAndHeadBucket() throws Exception {
    String bucketName = "bucket";
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headBucketRestCall(bucketName).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucketName).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        headBucketRestCall(bucketName).getResponseCode());
  }

  /**
   * Creates an existent bucket.
   */
  @Test
  public void createExistentBucket() throws Exception {
    String bucketName = "bucket";
//    Ensures this bucket exists.
    if (headBucketRestCall(bucketName).getResponseCode()
        == Response.Status.NOT_FOUND.getStatusCode()) {
      Assert.assertEquals(Response.Status.OK.getStatusCode(),
          createBucketRestCall(bucketName).getResponseCode());
    }

    HttpURLConnection connection = createBucketRestCall(bucketName);
    Assert.assertEquals(Response.Status.CONFLICT.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(bucketName, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.BUCKET_ALREADY_EXISTS, response.getCode());
  }

  /**
   * 2 users creates the same bucket.
   */
  @Test
  public void createSameBucket() throws Exception {
    String bucketName = "bucket";
//    Ensures this bucket exists.
    if (headBucketRestCall(bucketName, "user0").getResponseCode()
        == Response.Status.NOT_FOUND.getStatusCode()) {
      Assert.assertEquals(Response.Status.OK.getStatusCode(),
          createBucketRestCall(bucketName, "user0").getResponseCode());
    }

    HttpURLConnection connection = createBucketRestCall(bucketName, "user1");

    Assert.assertEquals(Response.Status.CONFLICT.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(bucketName, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.BUCKET_ALREADY_EXISTS, response.getCode());
  }
}
