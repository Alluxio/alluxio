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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.file.options.DescendantType;
import alluxio.underfs.UfsLoadResult;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemTestUtil;
import alluxio.underfs.options.ListOptions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.collect.Iterators;
import org.apache.commons.io.IOUtils;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.Executors;

/**
 * Unit tests for the {@link S3AUnderFileSystem} using a s3 mock server.
 */
public class S3AUnderFileSystemMockServerTest {
  private static final InstancedConfiguration CONF = Configuration.copyGlobal();

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_FILE = "test_file";
  private static final AlluxioURI TEST_FILE_URI = new AlluxioURI("s3://test-bucket/test_file");
  private static final String TEST_CONTENT = "test_content";

  private S3AUnderFileSystem mS3UnderFileSystem;
  private AmazonS3 mClient;

  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      // This is a must to close the behavior gap between native s3 and s3 proxy
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws AmazonClientException {
    AwsClientBuilder.EndpointConfiguration
        endpoint = new AwsClientBuilder.EndpointConfiguration(
        "http://localhost:8001", "us-west-2");
    mClient = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(mS3Proxy.getUri().toString(),
                Regions.US_WEST_2.getName()))
        .build();
    S3AsyncClient asyncClient =
        S3AsyncClient.builder().credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
            .endpointOverride(mS3Proxy.getUri()).region(Region.US_WEST_2).build();
    mClient.createBucket(TEST_BUCKET);

    mS3UnderFileSystem =
        new S3AUnderFileSystem(new AlluxioURI("s3://" + TEST_BUCKET), mClient,
            asyncClient, TEST_BUCKET,
            Executors.newSingleThreadExecutor(), new TransferManager(),
            UnderFileSystemConfiguration.defaults(CONF), false, false);
  }

  @After
  public void after() {
    mClient = null;
  }

  @Test
  public void read() throws IOException {
    mClient.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    InputStream is =
        mS3UnderFileSystem.open(TEST_FILE_URI.getPath());
    assertEquals(TEST_CONTENT, IOUtils.toString(is, StandardCharsets.UTF_8));
  }

  @Test
  public void nestedDirectory() throws Throwable {
    mClient.putObject(TEST_BUCKET, "d1/d1/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d1/d1/f2", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d1/d2/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d2/d1/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d3/", "");
    mClient.putObject(TEST_BUCKET, "d4/", "");
    mClient.putObject(TEST_BUCKET, "d4/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "f2", TEST_CONTENT);

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
       d4/
       d4/f1
       f1
       f2
     */

    UfsStatus[] ufsStatuses = mS3UnderFileSystem.listStatus(
        "/", ListOptions.defaults().setRecursive(true));
    assertNotNull(ufsStatuses);
    assertEquals(14, ufsStatuses.length);

    UfsLoadResult result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "/", DescendantType.ALL);
    Assert.assertEquals(9, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "/", DescendantType.ONE);
    Assert.assertEquals(6, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "d1", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "d1/", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "d3", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "d3/", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "d4", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "d4/", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "f1", DescendantType.NONE);
    assertEquals(1, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "f1/", DescendantType.NONE);
    assertEquals(0, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "f3", DescendantType.NONE);
    assertEquals(0, result.getItemsCount());

    result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mS3UnderFileSystem, "f3/", DescendantType.NONE);
    assertEquals(0, result.getItemsCount());
  }

  @Test
  public void iterator() throws IOException {
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 5; ++j) {
        for (int k = 0; k < 5; ++k) {
          mClient.putObject(TEST_BUCKET, String.format("%d/%d/%d", i, j, k), TEST_CONTENT);
        }
      }
    }

    Iterator<UfsStatus> ufsStatusesIterator = mS3UnderFileSystem.listStatusIterable(
        "/", ListOptions.defaults().setRecursive(true), null, 5);
    UfsStatus[] statusesFromListing =
        mS3UnderFileSystem.listStatus("/", ListOptions.defaults().setRecursive(true));
    assertNotNull(statusesFromListing);
    assertNotNull(ufsStatusesIterator);
    UfsStatus[] statusesFromIterator =
        Iterators.toArray(ufsStatusesIterator, UfsStatus.class);
    Arrays.sort(statusesFromListing, Comparator.comparing(UfsStatus::getName));
    assertArrayEquals(statusesFromIterator, statusesFromListing);
  }
}
