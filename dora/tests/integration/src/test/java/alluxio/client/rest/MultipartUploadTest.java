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
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.proxy.s3.CompleteMultipartUploadRequest;
import alluxio.proxy.s3.CompleteMultipartUploadRequest.Part;
import alluxio.proxy.s3.CompleteMultipartUploadResult;
import alluxio.proxy.s3.InitiateMultipartUploadResult;
import alluxio.proxy.s3.ListPartsResult;
import alluxio.proxy.s3.S3RestUtils;
import alluxio.s3.S3ErrorCode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableMap;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.Response.Status;

public class MultipartUploadTest extends RestApiTest {
  private FileSystem mFileSystem;
  private AmazonS3 mS3Client = null;
  private static final int UFS_PORT = 8004;
  private static final String S3_USER_NAME = "CustomersName@amazon.com";
  private static final String BUCKET_NAME = "bucket";
  private static final String OBJECT_NAME = "object";
  private static final String OBJECT_KEY = BUCKET_NAME + AlluxioURI.SEPARATOR + OBJECT_NAME;
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
          .setProperty(PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_MIN_PART_SIZE, "1KB")
          //Each part must be at least 1 KB in size, except the last part
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
   * Create a bucket and initiate a multipart upload under path "bucket/object".
   *
   * @return the upload id
   */
  public String initiateMultipartUpload() throws Exception {
    // Initiate the multipart upload.
    createBucketTestCase(BUCKET_NAME);
    final InitiateMultipartUploadResult result =
        initiateMultipartUploadTestCase(OBJECT_KEY)
            .getResponse(InitiateMultipartUploadResult.class);
    final String uploadId = result.getUploadId();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + OBJECT_KEY + "_" + uploadId);
    final URIStatus mpTempDirStatus = mFileSystem.getStatus(tmpDir);
    final URIStatus mpMetaFileStatus = mFileSystem.getStatus(
        new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)));

    Assert.assertEquals(BUCKET_NAME, result.getBucket());
    Assert.assertEquals(OBJECT_NAME, result.getKey());
    Assert.assertTrue(mpMetaFileStatus.isCompleted());
    Assert.assertTrue(mpTempDirStatus.isCompleted());
    Assert.assertTrue(mpTempDirStatus.getFileInfo().isFolder());
    return uploadId;
  }

  /**
   * Upload parts.
   *
   * @param uploadId the upload id
   * @param objects  the objects to upload
   * @param parts    the list of part number
   */
  public void uploadParts(String uploadId, List<String> objects, List<Integer> parts)
      throws Exception {
    // Upload parts
    for (int partNum : parts) {
      uploadPartTestCase(OBJECT_KEY, objects.get(partNum - 1).getBytes(), uploadId, partNum)
          .checkResponseCode(Status.OK.getStatusCode());
    }
    for (int partNum : parts) {
      getTestCase(OBJECT_KEY + "_" + uploadId + AlluxioURI.SEPARATOR + partNum)
          .checkResponseCode(Status.OK.getStatusCode())
          .checkResponse(objects.get(partNum - 1).getBytes());
    }
  }

  /**
   * upload parts with non-existent upload id.
   *
   * @throws Exception
   */
  @Test
  public void uploadPartWithNonExistentUpload() throws Exception {
    uploadPartTestCase(OBJECT_KEY, EMPTY_CONTENT, "wrong", 1)
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
    initiateMultipartUpload();
    uploadPartTestCase(OBJECT_KEY, EMPTY_CONTENT, "wrong", 1)
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
  }

  /**
   * Complete multipart upload.
   *
   * @param uploadId the upload id
   * @param partList the list of part number
   * @throws Exception
   */
  public void completeMultipartUpload(String uploadId, List<Part> partList) throws Exception {
    CompleteMultipartUploadResult completeMultipartUploadResult =
        completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
            new CompleteMultipartUploadRequest(partList))
            .checkResponseCode(Status.OK.getStatusCode())
            .getResponse(CompleteMultipartUploadResult.class);

    // Verify that the response is expected.
    Assert.assertEquals(BUCKET_NAME, completeMultipartUploadResult.getBucket());
    Assert.assertEquals(OBJECT_NAME, completeMultipartUploadResult.getKey());
  }

  /**
   * Complete multipart upload with 50 objects.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUpload() throws Exception {
    final int partsNum = 50;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + OBJECT_KEY + "_" + uploadId);
    for (int i = 1; i <= partsNum; i++) {
      parts.add(i);
      partList.add(new Part("", i));
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);

    uploadParts(uploadId, objects, parts);
    // Verify that all parts are uploaded to the temporary directory.
    Assert.assertEquals(partsNum, mFileSystem.listStatus(tmpDir).size());

    completeMultipartUpload(uploadId, partList);
    // Verify that the temporary directory is deleted.
    Assert.assertFalse(mFileSystem.exists(tmpDir));
    getTestCase(OBJECT_KEY).checkResponse(String.join("", objects).getBytes());
  }

  /**
   * Complete multipart upload with an empty part list.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUploadWithEmptyPart() throws Exception {
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.MALFORMED_XML);
  }

  /**
   * Complete multipart upload with the subsequence of uploaded parts.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUploadWithPartialParts() throws Exception {
    final int partsNum = 3;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    for (int i = 1; i <= partsNum; i++) {
      parts.add(i);
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);
    partList.add(new Part("", 1));
    partList.add(new Part("", 3));
    completeMultipartUpload(uploadId, partList);
    getTestCase(OBJECT_KEY).checkResponse(
        (objects.get(0) + objects.get(2)).getBytes());
  }

  /**
   * Complete multipart upload with non-existent part number.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUploadWithInvalidPart() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + OBJECT_KEY + "_" + uploadId);
    for (int i = 1; i <= partsNum; i++) {
      parts.add(i);
      partList.add(new Part("", i));
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);

    // Invalid part
    partList.add(new Part("", partsNum + 1));
    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.INVALID_PART);
    // the temporary directory should still exist.
    Assert.assertTrue(mFileSystem.exists(tmpDir));
  }

  /**
   * Complete multipart upload with invalid part order.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUploadWithInvalidPartOrder() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + OBJECT_KEY + "_" + uploadId);
    for (int i = 1; i <= partsNum; i++) {
      parts.add(i);
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);

    // Invalid part order
    partList.add(new Part("", 2));
    partList.add(new Part("", 1));

    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.INVALID_PART_ORDER);
    // the temporary directory should still exist.
    Assert.assertTrue(mFileSystem.exists(tmpDir));

    // Invalid part order
    partList.clear();
    partList.add(new Part("", 2));
    partList.add(new Part("", 2));

    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.INVALID_PART_ORDER);
    // the temporary directory should still exist.
    Assert.assertTrue(mFileSystem.exists(tmpDir));
  }

  /**
   * Complete multipart upload with the part size smaller than the minimum.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUploadWithTooSmallEntity() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    for (int i = 1; i <= partsNum; i++) {
      parts.add(i);
      partList.add(new Part("", i));
      objects.add(CommonUtils.randomAlphaNumString(1));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);

    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.ENTITY_TOO_SMALL);
  }

  /**
   * Complete multipart upload with non-existent upload id.
   *
   * @throws Exception
   */
  @Test
  public void completeMultipartUploadWithNonExistentUpload() throws Exception {
    final String uploadId = "wrong";
    final List<Part> partList = new ArrayList<>();

    initiateMultipartUpload();
    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
  }

  /**
   * Abort multipart upload.
   *
   * @throws Exception
   */
  @Test
  public void abortMultipartUpload() throws Exception {
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + OBJECT_KEY + "_" + uploadId);
    final List<Part> partList = new ArrayList<>();
    abortMultipartUploadTestCase(OBJECT_KEY, uploadId)
        .checkResponseCode(Status.NO_CONTENT.getStatusCode());
    completeMultipartUploadTestCase(OBJECT_KEY, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
    Assert.assertFalse(mFileSystem.exists(tmpDir));
  }

  /**
   * Abort multipart upload with non-existent upload id.
   *
   * @throws Exception
   */
  @Test
  public void abortMultipartUploadWithNonExistentUpload() throws Exception {
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + OBJECT_KEY + "_" + uploadId);
    abortMultipartUploadTestCase(OBJECT_KEY, "wrong")
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
    // the temporary directory should still exist.
    Assert.assertTrue(mFileSystem.exists(tmpDir));
  }

  /**
   * List parts.
   *
   * @throws Exception
   */
  @Test
  public void listParts() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    for (int i = 1; i <= partsNum; i++) {
      parts.add(i);
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);

    final ListPartsResult listPartsResult =
        listTestCase(OBJECT_KEY, ImmutableMap.of("uploadId", uploadId))
            .checkResponseCode(Status.OK.getStatusCode())
            .getResponse(ListPartsResult.class);

    Assert.assertEquals(AlluxioURI.SEPARATOR + BUCKET_NAME, listPartsResult.getBucket());
    Assert.assertEquals(OBJECT_NAME, listPartsResult.getKey());
    Assert.assertEquals(uploadId, listPartsResult.getUploadId());
    Assert.assertEquals(partsNum, listPartsResult.getParts().size());
    for (int i = 0; i < partsNum; i++) {
      Assert.assertEquals(i + 1, listPartsResult.getParts().get(i).getPartNumber());
      Assert.assertEquals(Constants.KB, listPartsResult.getParts().get(i).getSize());
    }
  }

  /**
   * List parts with non-existent upload id.
   *
   * @throws Exception
   */
  @Test
  public void listPartsWithNonExistentUpload() throws Exception {
    final String uploadId = initiateMultipartUpload();
    listTestCase(OBJECT_KEY, ImmutableMap.of("uploadId", "wrong"))
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
    listTestCase(OBJECT_KEY + AlluxioURI.SEPARATOR, ImmutableMap.of("uploadId", uploadId))
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
  }

  /**
   * List parts with non-existent bucket.
   *
   * @throws Exception
   */
  @Test
  public void listPartsWithNonExistentBucket() throws Exception {
    final String uploadId = initiateMultipartUpload();
    String objectKey = "wrong" + AlluxioURI.SEPARATOR + OBJECT_NAME;
    listTestCase(objectKey, ImmutableMap.of("uploadId", uploadId))
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_BUCKET);
  }

  /**
   * Get default options with username {@code CustomersName@amazon.com}.
   *
   * @return
   */
  @Override
  protected TestCaseOptions getDefaultOptionsWithAuth() {
    return getDefaultOptionsWithAuth(S3_USER_NAME);
  }
}
