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

package alluxio.underfs.s3a;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListOptions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Unit tests for the {@link S3AUnderFileSystem} using an s3 mock server.
 */
public class S3AUnderFileSystemMockServerTest {
  private static final InstancedConfiguration CONF = Configuration.copyGlobal();

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_FILE = "test_file";
  private static final AlluxioURI TEST_FILE_URI = new AlluxioURI("s3://test-bucket/test_file");
  private static final String TEST_CONTENT = "test_content";

  private S3AUnderFileSystem mS3UnderFileSystem;
  private AmazonS3 mClient;

  private S3Mock mS3MockServer;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws AmazonClientException {
    mS3MockServer = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    mS3MockServer.start();

    AwsClientBuilder.EndpointConfiguration
        endpoint = new AwsClientBuilder.EndpointConfiguration(
        "http://localhost:8001", "us-west-2");
    mClient = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(endpoint)
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();
    mClient.createBucket(TEST_BUCKET);
    mS3UnderFileSystem =
        new S3AUnderFileSystem(new AlluxioURI("s3://" + TEST_BUCKET), mClient, TEST_BUCKET,
            Executors.newSingleThreadExecutor(), new TransferManager(),
            UnderFileSystemConfiguration.defaults(CONF), false);
  }

  @After
  public void after() {
    mClient = null;
    try {
      if (mS3MockServer != null) {
        mS3MockServer.shutdown();
      }
    } finally {
      mS3MockServer = null;
    }
  }

  @Test
  public void read() throws IOException {
    mClient.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    InputStream is =
        mS3UnderFileSystem.open(TEST_FILE_URI.getPath());
    assertEquals(TEST_CONTENT, IOUtils.toString(is, StandardCharsets.UTF_8));
  }

  @Test
  public void listRecursive() throws IOException {
    mClient.putObject(TEST_BUCKET, "d1/d1/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d1/d1/f2", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d1/d2/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d2/d1/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d3/", "");
    mClient.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "f2", TEST_CONTENT);

    UfsStatus[] ufsStatuses = mS3UnderFileSystem.listStatus(
        "/", ListOptions.defaults().setRecursive(true));

    /*
      Objects:
       d1/
       d1/d1/
       d1/d1/f1
       d1/d1/f2
       d1/d2/
       d1/d2/f1
       d2/
       d2/d1/
       d2/d1/f1
       d3/
       f1
       f2
     */
    assertNotNull(ufsStatuses);
    assertEquals(12, ufsStatuses.length);
  }
}
