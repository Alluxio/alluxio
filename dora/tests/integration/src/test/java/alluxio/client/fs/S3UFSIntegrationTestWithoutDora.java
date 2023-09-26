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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;

public class S3UFSIntegrationTestWithoutDora {
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  private static final String TEST_BUCKET = "test-bucket";
  public static final String TEST_FILENAME = "s3://" + TEST_BUCKET + "/test-file";
  public static final String TEST_BUCKET_URI = "s3://" + TEST_BUCKET + "/";
  public static final Path TEST_PATH = new Path(TEST_FILENAME);
  private static final String TEST_CONTENT = "test-content";
  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  private AmazonS3 mS3Client = null;
  private FileSystem mAlluxioS3Client;

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

    Configuration conf = new Configuration();
    conf.set("fs.s3.impl", "alluxio.hadoop.FileSystem");
    conf.set("fs.AbstractFileSystem.s3.impl", "alluxio.hadoop.AlluxioFileSystem");
    conf.setBoolean(PropertyKey.DORA_ENABLED.getName(), false);
    conf.set(PropertyKey.S3A_ACCESS_KEY.getName(), mS3Proxy.getAccessKey());
    conf.set(PropertyKey.S3A_SECRET_KEY.getName(), mS3Proxy.getSecretKey());
    conf.set(PropertyKey.UNDERFS_S3_ENDPOINT.getName(), "localhost:8001");
    conf.set(PropertyKey.UNDERFS_S3_ENDPOINT_REGION.getName(), "us-west-2");
    conf.setBoolean(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS.getName(), true);

    mAlluxioS3Client = FileSystem.get(new URI(TEST_BUCKET_URI), conf);
  }

  @Test
  public void testReadWrite() throws Exception {
    FSDataOutputStream out = mAlluxioS3Client.create(TEST_PATH);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();

    FSDataInputStream in = mAlluxioS3Client.open(TEST_PATH);
    byte[] buffReadFromDora = new byte[buf.length];
    in.read(buffReadFromDora);
    in.close();
    assertEquals(Arrays.toString(buf), Arrays.toString(buffReadFromDora));
    mAlluxioS3Client.delete(TEST_PATH, false);
  }

  @Test
  public void testGetFileStatus() throws Exception {
    FSDataOutputStream out = mAlluxioS3Client.create(TEST_PATH);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();

    FSDataInputStream in = mAlluxioS3Client.open(TEST_PATH);
    byte[] buffReadFromDora = new byte[buf.length];
    in.read(buffReadFromDora);
    in.close();
    FileStatus fileStatus = mAlluxioS3Client.getFileStatus(TEST_PATH);
    assertEquals(buf.length, fileStatus.getLen());
    BlockLocation[] blockLocations =
        mAlluxioS3Client.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    assertEquals("localhost", blockLocations[0].getHosts()[0]);
    mAlluxioS3Client.delete(TEST_PATH, false);
  }

  @Test
  public void testListStatus() throws Exception {
    String testDirName = "testdir";
    String testFileName = "testfile";
    Path fullDirPath = new Path(TEST_BUCKET_URI + testDirName);
    Path fullFilePath = new Path(TEST_BUCKET_URI + testFileName);
    mAlluxioS3Client.mkdirs(fullDirPath);
    FSDataOutputStream out = mAlluxioS3Client.create(fullFilePath);
    out.close();

    Path pathToList = new Path(TEST_BUCKET_URI);
    FileStatus[] fileStatuses = mAlluxioS3Client.listStatus(pathToList);
    assertEquals(fullFilePath, fileStatuses[0].getPath());
    assertFalse(fileStatuses[0].isDirectory());
    assertEquals(fullDirPath, fileStatuses[1].getPath());
    assertTrue(fileStatuses[1].isDirectory());

    mAlluxioS3Client.delete(fullDirPath, true);
    mAlluxioS3Client.delete(fullFilePath, false);
  }
}
