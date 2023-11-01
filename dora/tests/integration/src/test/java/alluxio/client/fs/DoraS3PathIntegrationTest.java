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

import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.conf.PropertyKey;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Integration tests for Alluxio Client (reuse the {@link LocalAlluxioCluster}).
 */
@Ignore
@DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "beinan",
    comment = "fix it.")
public final class DoraS3PathIntegrationTest extends BaseIntegrationTest {
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  LocalAlluxioClusterResource.Builder mLocalAlluxioClusterResourceBuilder =
      new LocalAlluxioClusterResource.Builder()
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
  public static final String TEST_FILENAME = "s3://" + TEST_BUCKET + "/test-file";
  public static final Path TEST_PATH = new Path(TEST_FILENAME);
  private static final String TEST_CONTENT = "test-content";

  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  private AmazonS3 mS3Client = null;
  private FileSystem mHadoopFileSystem;
  private LocalAlluxioClusterResource mClusterResource;

  @Before
  public void before() throws Exception {
    mClusterResource = mLocalAlluxioClusterResourceBuilder.build();
    mClusterResource.start();

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

    Configuration conf = new Configuration();
    conf.set("fs.s3.impl", "alluxio.hadoop.FileSystem");
    conf.set("fs.AbstractFileSystem.s3.impl", "alluxio.hadoop.AlluxioFileSystem");

    mHadoopFileSystem = FileSystem.get(new URI("s3://test-bucket/"), conf);
  }

  @After
  public void after() throws Exception {
    mClusterResource.stop();
  }

  @Test
  public void testReadWrite() throws Exception
  {
    FSDataOutputStream out = mHadoopFileSystem.create(TEST_PATH);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();

    FSDataInputStream in = mHadoopFileSystem.open(TEST_PATH);
    byte[] buffReadFromDora = new byte[buf.length];
    in.read(buffReadFromDora);
    in.close();
    assertEquals(Arrays.toString(buf), Arrays.toString(buffReadFromDora));
  }

  @Test
  public void testGetFileStatus() throws Exception
  {
    FSDataOutputStream out = mHadoopFileSystem.create(TEST_PATH);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();

    FSDataInputStream in = mHadoopFileSystem.open(TEST_PATH);
    byte[] buffReadFromDora = new byte[buf.length];
    in.read(buffReadFromDora);
    in.close();
    assertEquals(buf.length, mHadoopFileSystem.getFileStatus(TEST_PATH).getLen());
  }
}
