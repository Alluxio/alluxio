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
import alluxio.proxy.s3.S3Constants;
import alluxio.proxy.s3.S3Error;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.net.HttpURLConnection;
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

  @Test
  public void putAndGetSmallObject() throws Exception {
    final String bucket = "bucket";
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  @Ignore
  public void putAndGetLargeObject() throws Exception {
//    TODO(Xinran Dong): error occurs when getting large object.
  }

  @Test
  public void putAndHeadDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = bucket + AlluxioURI.SEPARATOR + "folder" + AlluxioURI.SEPARATOR;

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(folderKey, EMPTY_OBJECT).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(folderKey).checkResponseCode(Status.OK.getStatusCode());
  }

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

  @Test
  public void overwriteObject() throws Exception {
    final String bucket = "bucket";
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();
    final byte[] object2 = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
//   This object will be overwritten.
    createObjectTestCase(objectKey, object2).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object2);
  }

  /**
   * Creates a folder which path is the same as the object.
   */
  @Test
  public void putObjectAndDirectoryWithSamePath() throws Exception {
    final String bucket = "bucket";
    final String objectKey = "bucket/object";
    final String folderKey = "bucket/object/";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
    createObjectTestCase(folderKey, EMPTY_OBJECT).checkResponseCode(Status.OK.getStatusCode());

    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
    headTestCase(folderKey).checkResponseCode(Status.OK.getStatusCode());
  }

  @Test
  public void getNonexistentObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());

    getTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  @Test
  public void getNonexistentDirectory() throws Exception {
    final String bucket = "bucket" + AlluxioURI.SEPARATOR;
    final String objectKey = bucket + "object" + AlluxioURI.SEPARATOR;

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_KEY);
  }

  @Test
  public void putDirectoryToNonexistentBucket() throws Exception {

    final String bucket = "non-existent-bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";
    final byte[] object = new byte[] {};
    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  @Test
  public void putObjectToNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    headTestCase(bucket).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  @Test
  public void putObjectWithWrongMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    newTestCase(objectKey, NO_PARAMS,
        HttpMethod.PUT, getDefaultOptionsWithAuth()
            .setBody(object).setMD5("")).checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.BAD_DIGEST);
  }

  @Test
  public void putObjectWithoutMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    newTestCase(objectKey, NO_PARAMS,
        HttpMethod.PUT, getDefaultOptionsWithAuth()
            .setBody(object)).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObjectToAnotherBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String sourcePath = "bucket1/object";
    final String targetPath = "bucket2/object";

    final byte[] object = "Hello World!".getBytes();
    createBucketTestCase(bucket1).checkResponseCode(Status.OK.getStatusCode());
    createBucketTestCase(bucket2).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());

    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObjectToNonexistentBucket() throws Exception {
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
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObjectToAnotherFolder() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/sourceDir/object";
    final String targetPath = "bucket/targetDir/object";

    final byte[] object = "Hello World!".getBytes();
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());

    // copy object
    copyObjectTestCase(sourcePath, targetPath).checkResponseCode(Status.OK.getStatusCode());
    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  /**
   * Copies an object and renames it.
   */
  @Test
  public void copyObjectAsAnotherObject() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/object1";
    final String targetPath = "bucket/object2";

    final byte[] object = "Hello World!".getBytes();
    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(sourcePath, object).checkResponseCode(Status.OK.getStatusCode());
    // copy object
    new TestCase(mHostname, mPort, mBaseUri,
        targetPath,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourcePath)).runAndGetResponse();

    getTestCase(targetPath).checkResponseCode(Status.OK.getStatusCode()).checkResponse(object);
  }

  @Test
  public void deleteObjectAndDirectory() throws Exception {
    final String bucket = "bucket" + AlluxioURI.SEPARATOR;
    final String folderKey = bucket + "folder" + AlluxioURI.SEPARATOR;
    final String objectKey = folderKey + "object";
    final byte[] object = "Hello World!".getBytes();

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());

//    The directory can't be deleted because the directory is not empty.
    deleteTestCase(folderKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode());

//    Deletes the object first.
    deleteTestCase(objectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(folderKey).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(folderKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void deleteObjectInNonexistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";

//    deleteRestCall(objectKey);
    deleteTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  @Test
  public void deleteNonexistentObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(objectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
  }

  @Test
  public void deleteDirectoryInNonexistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";

    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        objectKey, NO_PARAMS, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).execute();
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(),
        connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  @Test
  public void deleteNonexistentDirectory() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());
    deleteTestCase(objectKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
  }

  @Test
  public void deleteNonEmptyDirectory() throws Exception {
    final String bucket = "bucket" + AlluxioURI.SEPARATOR;
    final String folderKey = bucket + "folder" + AlluxioURI.SEPARATOR;
    final String objectKey = folderKey + "object";
    final byte[] object = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
    headTestCase(objectKey).checkResponseCode(Status.NOT_FOUND.getStatusCode());

    createBucketTestCase(bucket).checkResponseCode(Status.OK.getStatusCode());
    createObjectTestCase(objectKey, object).checkResponseCode(Status.OK.getStatusCode());

//    The directory can't be deleted because the directory is not empty.
    deleteTestCase(folderKey).checkResponseCode(Status.NO_CONTENT.getStatusCode());
    headTestCase(objectKey).checkResponseCode(Status.OK.getStatusCode());
  }
}
