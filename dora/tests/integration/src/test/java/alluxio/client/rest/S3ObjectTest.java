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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response.Status;

public class S3ObjectTest extends RestApiTest {
  private FileSystem mFileSystem;
  private AmazonS3 mS3Client = null;
  private static final int UFS_PORT = 8003;
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
          .setProperty(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL,
              "0s")  //always sync the metadata
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
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() {
    mS3Client = null;
  }

  /**
   * Gets a non-existent object.
   */
  @Test
  public void getNonExistentObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  /**
   * Gets a deleted object.
   */
  @Test
  public void getDeletedObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey, EMPTY_CONTENT).checkResponseCode(Status.OK.getStatusCode());
    mS3Client.deleteObject(TEST_BUCKET, fullKey);
    getTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  /**
   * Heads a non-existent directory.
   */
  @Test
  public void headNonExistentDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = "folder";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + folderKey + AlluxioURI.SEPARATOR;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }

  /**
   * Heads a deleted directory.
   */
  @Test
  public void headDeletedDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = "folder";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + folderKey + AlluxioURI.SEPARATOR;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey, EMPTY_CONTENT).checkResponseCode(Status.OK.getStatusCode());
    mS3Client.deleteObject(TEST_BUCKET, fullKey);
    headTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }

  /**
   * Puts a small object and gets it.
   */
  @Test
  public void putAndGetSmallObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey, content).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(fullKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content)
        .checkHeader(S3Constants.S3_CONTENT_TYPE_HEADER,
            TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
        .checkHeader(S3Constants.S3_CONTENT_LENGTH_HEADER,
            String.valueOf(content.length));
  }

  /**
   * Puts a large object and gets it.
   */
  @Ignore
  public void putAndGetLargeObject() throws Exception {
    // TODO(Xinran Dong): error occurs when getting large object.
  }

  /**
   * Puts and heads a multi-level directory.
   */
  @Test
  public void putAndHeadDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey1 = "folder1";
    final String folderKey2 = "folder2";
    final String fullKey1 = bucket + AlluxioURI.SEPARATOR + folderKey1 + AlluxioURI.SEPARATOR;
    final String fullKey2 = bucket + AlluxioURI.SEPARATOR + folderKey1 + AlluxioURI.SEPARATOR
        + folderKey2 + AlluxioURI.SEPARATOR;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey2, EMPTY_CONTENT).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(fullKey1).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(fullKey2).checkResponseCode(Status.OK.getStatusCode());
  }

  /**
   * Overwrites an existent object.
   */
  @Test
  public void overwriteObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();
    final byte[] content2 = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey, content).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(fullKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content);
    // overwrite this object
    createObjectTestCase(fullKey, content2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(fullKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content2)
        .checkHeader(S3Constants.S3_CONTENT_LENGTH_HEADER,
            String.valueOf(content2.length));
  }

  /**
   * Puts a directory to a non-existent bucket.
   */
  @Test
  public void putDirectoryToNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";
    final String folderKey = "folder";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + folderKey + AlluxioURI.SEPARATOR;

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createObjectTestCase(fullKey, EMPTY_CONTENT).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Puts an object to a non-existent bucket.
   */
  @Test
  public void putObjectToNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createObjectTestCase(fullKey, content).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Puts an object to a deleted bucket.
   */
  @Test
  public void putObjectToDeletedBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    // deletes bucket from ufs
    mS3Client.deleteObject(TEST_BUCKET, bucket + AlluxioURI.SEPARATOR);
    createObjectTestCase(fullKey, content).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Puts an object with wrong MD5.
   */
  @Test
  public void putObjectWithWrongMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey, getDefaultOptionsWithAuth().setBody(content).setMD5(""))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.BAD_DIGEST);
  }

  /**
   * Puts an object without MD5.
   */
  @Test
  public void putObjectWithoutMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullKey, getDefaultOptionsWithAuth().setBody(content))
        .checkResponseCode(Status.OK.getStatusCode());
    getTestCase(fullKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content);
  }

  /**
   * Copies an object and renames it.
   */
  @Test
  public void copyObjectAndRename() throws Exception {
    final String bucket = "bucket";
    final String objectKey1 = "object1";
    final String objectKey2 = "object2";
    final String sourcePath = bucket + AlluxioURI.SEPARATOR + objectKey1;
    final String targetPath = bucket + AlluxioURI.SEPARATOR + objectKey2;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());
    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content)
        .checkHeader(S3Constants.S3_CONTENT_TYPE_HEADER,
            TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
        .checkHeader(S3Constants.S3_CONTENT_LENGTH_HEADER,
            String.valueOf(content.length));
  }

  /**
   * Copies a non-existent object.
   */
  @Test
  public void copyNonExistentObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey1 = "object1";
    final String objectKey2 = "object2";
    final String sourcePath = bucket + AlluxioURI.SEPARATOR + objectKey1;
    final String targetPath = bucket + AlluxioURI.SEPARATOR + objectKey2;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  /**
   * Copies a deleted object.
   */
  @Test
  public void copyDeletedObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey1 = "object1";
    final String objectKey2 = "object2";
    final String sourcePath = bucket + AlluxioURI.SEPARATOR + objectKey1;
    final String targetPath = bucket + AlluxioURI.SEPARATOR + objectKey2;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());
    mS3Client.deleteObject(TEST_BUCKET, sourcePath);
    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  /**
   * Copies an object and overwrites another object.
   */
  @Test
  public void copyObjectAndOverwrite() throws Exception {
    final String bucket = "bucket";
    final String objectKey1 = "object1";
    final String objectKey2 = "object2";
    final String sourcePath = bucket + AlluxioURI.SEPARATOR + objectKey1;
    final String targetPath = bucket + AlluxioURI.SEPARATOR + objectKey2;
    final byte[] content = "Hello World!".getBytes();
    final byte[] content2 = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(targetPath, content2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content2);
    // copy object1 and overwrite object2.
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content);
  }

  /**
   * Copies an object to a non-existent bucket.
   */
  @Test
  public void copyObjectToNonExistentBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String objectKey = "object";
    final String sourcePath = bucket1 + AlluxioURI.SEPARATOR + objectKey;
    final String targetPath = bucket2 + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket1).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(bucket2).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Copies an object to a deleted bucket.
   */
  @Test
  public void copyObjectToDeletedBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String objectKey = "object";
    final String sourcePath = bucket1 + AlluxioURI.SEPARATOR + objectKey;
    final String targetPath = bucket2 + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket1).checkResponseCode(Status.OK.getStatusCode());
    createBucketTestCase(bucket2).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());
    mS3Client.deleteObject(TEST_BUCKET, bucket2 + AlluxioURI.SEPARATOR);
    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Copies an object from one bucket to another bucket.
   * TODO(Xinran Dong): We need to support the target path is a directory.
   */
  @Ignore
  public void copyObjectToAnotherBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String objectKey = "object";
    final String sourcePath = bucket1 + AlluxioURI.SEPARATOR + objectKey;
    final String targetPath = bucket2 + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket1).checkResponseCode(Status.OK.getStatusCode());
    createBucketTestCase(bucket2).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());

    copyObjectTestCase(sourcePath, bucket2 + AlluxioURI.SEPARATOR).checkResponseCode(
        Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content);
  }

  /**
   * Copies an object from one folder to a different folder.
   * TODO(Xinran Dong): We need to support the target path is a directory.
   */
  @Ignore
  public void copyObjectToAnotherDir() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String folderKey1 = "sourceDir";
    final String folderKey2 = "targetDir";
    final String sourcePath =
        bucket + AlluxioURI.SEPARATOR + folderKey1 + AlluxioURI.SEPARATOR + objectKey;
    final String targetPath =
        bucket + AlluxioURI.SEPARATOR + folderKey2 + AlluxioURI.SEPARATOR + objectKey;
    final String fullFolderKey2 =
        bucket + AlluxioURI.SEPARATOR + folderKey2 + AlluxioURI.SEPARATOR;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, content).checkResponseCode(Status.OK.getStatusCode());
    copyObjectTestCase(sourcePath, fullFolderKey2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(content);
  }

  /**
   * Deletes objects in the directory and then the directory.
   */
  @Test
  public void deleteObjectAndDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = "folder";
    final String objectKey = "object";
    final String fullFolderKey =
        bucket + AlluxioURI.SEPARATOR + folderKey;
    final String fullObjectKey =
        bucket + AlluxioURI.SEPARATOR + folderKey + AlluxioURI.SEPARATOR + objectKey;
    final byte[] content = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(fullObjectKey, content).checkResponseCode(Status.OK.getStatusCode());

    // The directory can't be deleted because the directory is not empty.
    deleteTestCase(fullFolderKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(fullObjectKey).checkResponseCode(Status.OK.getStatusCode());

    // Deletes the object first, then deletes the directory.
    deleteTestCase(fullObjectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(fullObjectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(fullObjectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(fullObjectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }

  /**
   * Deletes the object in a non-existent bucket.
   */
  @Test
  public void deleteObjectInNonExistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Deletes the object in a deleted bucket.
   */
  @Test
  public void deleteObjectInDeletedBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "object";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + objectKey;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    mS3Client.deleteObject(TEST_BUCKET, bucket + AlluxioURI.SEPARATOR);
    deleteTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Deletes the directory in a non-existent bucket.
   */
  @Test
  public void deleteDirectoryInNonExistentBucket() throws Exception {
    final String bucket = "bucket";
    final String folderKey = "folder";
    final String fullKey = bucket + AlluxioURI.SEPARATOR + folderKey + AlluxioURI.SEPARATOR;
    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(fullKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }
}
