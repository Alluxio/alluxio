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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.ExistsContext;
import alluxio.master.file.contexts.MountContext;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
public final class FileSystemMasterS3UfsTest extends FileSystemMasterTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMasterS3UfsTest.class);
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_FILE = "test_file";
  private static final String TEST_DIRECTORY = "test_directory";
  private static final String TEST_CONTENT = "test_content";
  private static final AlluxioURI UFS_ROOT = new AlluxioURI("s3://test-bucket/");
  private static final AlluxioURI MOUNT_POINT = new AlluxioURI("/s3_mount");
  private AmazonS3 mS3Client;
  private S3Mock mS3MockServer;

  @Override
  public void before() throws Exception {
    mS3MockServer = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    mS3MockServer.start();

    Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001");
    Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2");
    Configuration.set(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true);
    Configuration.set(PropertyKey.S3A_ACCESS_KEY, "_");
    Configuration.set(PropertyKey.S3A_SECRET_KEY, "_");

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

    super.before();
  }

  @Test
  public void basicWrite()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mFileSystemMaster.createDirectory(
        MOUNT_POINT.join(TEST_DIRECTORY),
        CreateDirectoryContext.defaults().setWriteType(WriteType.THROUGH)
    );
    assertEquals(1, mS3Client.listObjects(TEST_BUCKET).getObjectSummaries().size());
    assertNotNull(mS3Client.getObject(TEST_BUCKET, TEST_DIRECTORY + "/"));
  }

  @Test
  public void basicSync()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
    assertTrue(mFileSystemMaster.exists(MOUNT_POINT.join(TEST_FILE), ExistsContext.defaults()));
  }

  @Override
  public void after() throws Exception {
    mS3Client = null;
    try {
      if (mS3MockServer != null) {
        mS3MockServer.shutdown();
      }
    } finally {
      mS3MockServer = null;
    }
    super.after();
  }
}
