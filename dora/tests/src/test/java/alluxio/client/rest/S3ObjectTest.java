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
import alluxio.proxy.s3.S3ErrorCode;
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

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response.Status;

public class S3ObjectTest extends RestApiTest {
  private FileSystem mFileSystem;
  private static final String TEST_BUCKET = "test-bucket";
  private AmazonS3 mS3Client = null;
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8003)
      .withCredentials("_", "_")
      .build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeProxy(true)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8003")
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
    final String objectKey = "bucket/object";
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  /**
   * Gets a non-existent directory.
   */
  @Test
  public void getNonExistentDirectory() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object/";
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  /**
   * Puts and gets a small object.
   */
  @Test
  public void putAndGetSmallObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Puts and gets a large object.
   */
  @Ignore
  public void putAndGetLargeObject() throws Exception {
    // TODO(Xinran Dong): error occurs when getting large object.
  }

  /**
   * Puts and heads a directory.
   */
  @Test
  public void putAndHeadDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = bucket + AlluxioURI.SEPARATOR + "folder" + AlluxioURI.SEPARATOR;

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(folderKey, EMPTY_OBJECT).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(folderKey).checkResponseCode(Status.OK.getStatusCode());
  }

  /**
   * Puts and heads directories.
   */
  @Test
  public void putAndHeadDirectories() throws Exception {
    final String bucket = "bucket" + AlluxioURI.SEPARATOR;
    final String objectKey1 = bucket + "folder0" + AlluxioURI.SEPARATOR;
    final String objectKey2 = objectKey1 + "folder1" + AlluxioURI.SEPARATOR;
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey2, EMPTY_OBJECT).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(objectKey1).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(objectKey2).checkResponseCode(Status.OK.getStatusCode());
  }

  /**
   * Overwrites an existent object.
   */
  @Test
  public void overwriteObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";
    final byte[] object = "Hello World!".getBytes();
    final byte[] object2 = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
//   This object will be overwritten.
    createObjectTestCase(objectKey, object2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object2);
  }

  /**
   * Puts a directory to a non-existent bucket.
   */
  @Test
  public void putDirectoryToNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";
    final String objectKey = "non-existent-bucket/folder/";
    final byte[] object = new byte[] {};
    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Puts an object to a non-existent bucket.
   */
  @Test
  public void putObjectToNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";
    final String objectKey = "non-existent-bucket/object";
    final byte[] object = "Hello World!".getBytes();

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Puts an object with wrong MD5.
   */
  @Test
  public void putObjectWithWrongMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    newTestCase(objectKey, NO_PARAMS,
        HttpMethod.PUT, getDefaultOptionsWithAuth()
            .setBody(object).setMD5("")).checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.BAD_DIGEST);
  }

  /**
   * Puts an object without MD5.
   */
  @Test
  public void putObjectWithoutMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    newTestCase(objectKey, NO_PARAMS,
        HttpMethod.PUT, getDefaultOptionsWithAuth()
            .setBody(object)).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object and renames it.
   */
  @Test
  public void copyObjectAndRename() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/object1";
    final String targetPath = "bucket/object2";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());
    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object and overwrite another object.
   */
  @Test
  public void copyObjectAndOverwrite() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/object1";
    final String targetPath = "bucket/object2";
    final byte[] object = "Hello World!".getBytes();
    final byte[] object2 = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(targetPath, object2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object2);
    // copy object and overwrite another object.
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object to a non-existent bucket.
   */
  @Test
  public void copyObjectToNonExistentBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String sourcePath = "bucket1/object";
    final String targetPath = "bucket2/object";

    final byte[] object = "Hello World!".getBytes();
    createBucketTestCase(bucket1).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(bucket2).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Copies an object from one bucket to another bucket.
   */
  @Ignore
  public void copyObjectToAnotherBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2/";
    final String sourcePath = "bucket1/object";
    final String targetPath = "bucket2/object";

    final byte[] object = "Hello World!".getBytes();
    createBucketTestCase(bucket1).checkResponseCode(Status.OK.getStatusCode());
    createBucketTestCase(bucket2).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());

    // TODO(Xinran Dong): copy to a directory.
    copyObjectTestCase(sourcePath, bucket2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Ignore
  public void copyObjectToAnotherDir() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/sourceDir/object";
    final String targetFolder = "bucket/targetDir/";
    final String targetPath = "bucket/targetDir/object";

    final byte[] object = "Hello World!".getBytes();
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());

    // TODO(Xinran Dong): copy to a directory.
    copyObjectTestCase(sourcePath, targetFolder).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Deletes a directory.
   */
  @Test
  public void deleteObjectAndDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = "bucket/folder/";
    final String objectKey =  "bucket/folder/object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());

    // The directory can't be deleted because the directory is not empty.
    deleteTestCase(folderKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode());

    // Deletes the object first.
    deleteTestCase(objectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(folderKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(folderKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }

  /**
   * Deletes the object in a non-existent bucket.
   */
  @Test
  public void deleteObjectInNonExistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Deletes a non-existent object.
   */
  @Test
  public void deleteNonExistentObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(objectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }

  /**
   * Deletes the directory in a non-existent bucket.
   */
  @Test
  public void deleteDirectoryInNonExistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/folder/";
    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Deletes a non-existent directory.
   */
  @Test
  public void deleteNonExistentDirectory() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/folder/";

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(objectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }
}
