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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.rest.TestCaseOptions;
import alluxio.util.CommonUtils;
import alluxio.wire.FileInfo;

import com.google.common.io.BaseEncoding;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

/**
 * Test cases for {@link S3RestServiceHandler}.
 */
public final class S3ClientRestApiTest extends RestApiTest {
  private static final alluxio.master.file.options.GetStatusOptions GET_STATUS_OPTIONS =
      alluxio.master.file.options.GetStatusOptions.defaults();
  private static final Map<String, String> NO_PARAMS = new HashMap<>();

  private static final String S3_SERVICE_PREFIX = "s3";
  private static final String BUCKET_SEPARATOR = ":";

  private FileSystem mFileSystem;
  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getProxyProcess().getWebLocalPort();
    mFileSystemMaster = mResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
    mFileSystem = mResource.get().getClient();
  }

  @Test
  public void putBucket() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);
    // Verify the directory is created for the new bucket.
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void putBucketUnderMountPoint() throws Exception {
    final String mountPoint = "s3";
    final String bucketName = "bucket";
    final String s3Path = mountPoint + BUCKET_SEPARATOR + bucketName;

    AlluxioURI mountPointPath = new AlluxioURI(AlluxioURI.SEPARATOR + mountPoint);
    mFileSystemMaster.mount(mountPointPath, new AlluxioURI(mFolder.newFolder().getAbsolutePath()),
        MountOptions.defaults());

    // Create a new bucket under an existing mount point.
    createBucketRestCall(s3Path);

    // Verify the directory is created for the new bucket, under the mount point.
    AlluxioURI uri = new AlluxioURI(
        AlluxioURI.SEPARATOR + mountPoint + AlluxioURI.SEPARATOR + bucketName);
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void putBucketUnderNestedMountPoint() throws Exception {
    final String mountPointParent = "mounts";
    final String mountPointName = "s3";
    final String bucketName = "bucket";
    final String s3Path =
        mountPointParent + BUCKET_SEPARATOR + mountPointName + BUCKET_SEPARATOR + bucketName;

    mFileSystemMaster.createDirectory(new AlluxioURI(
        AlluxioURI.SEPARATOR + mountPointParent), CreateDirectoryOptions.defaults());
    AlluxioURI mountPointPath = new AlluxioURI(AlluxioURI.SEPARATOR + mountPointParent
        + AlluxioURI.SEPARATOR + mountPointName);
    mFileSystemMaster.mount(mountPointPath, new AlluxioURI(mFolder.newFolder().getAbsolutePath()),
        MountOptions.defaults());

    // Create a new bucket under an existing nested mount point.
    createBucketRestCall(s3Path);

    // Verify the directory is created for the new bucket, under the mount point.
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + mountPointParent
        + AlluxioURI.SEPARATOR + mountPointName + AlluxioURI.SEPARATOR + bucketName);
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void putBucketUnderNonExistingMountPoint() throws Exception {
    final String mountPoint = "s3";
    final String bucketName = "bucket";
    final String s3Path = mountPoint + BUCKET_SEPARATOR + bucketName;

    try {
      // Create a new bucket under a non-existing mount point should fail.
      createBucketRestCall(s3Path);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("create bucket under non-existing mount point should fail");
  }

  @Test
  public void putBucketUnderNonMountPointDirectory() throws Exception {
    final String dirName = "dir";
    final String bucketName = "bucket";
    final String s3Path = dirName + BUCKET_SEPARATOR + bucketName;

    AlluxioURI dirPath = new AlluxioURI(AlluxioURI.SEPARATOR + dirName);
    mFileSystemMaster.createDirectory(dirPath, CreateDirectoryOptions.defaults());

    try {
      // Create a new bucket under a non-mount-point directory should fail.
      createBucketRestCall(s3Path);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("create bucket under non-mount-point directory should fail");
  }

  @Test
  public void deleteBucket() throws Exception {
    final String bucket = "bucket-to-delete";
    createBucketRestCall(bucket);

    // Verify the directory is created for the new bucket.
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());

    HttpURLConnection connection = deleteBucketRestCall(bucket);
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), connection.getResponseCode());

    try {
      mFileSystemMaster.getFileInfo(uri, GET_STATUS_OPTIONS);
    } catch (FileDoesNotExistException e) {
      // expected
      return;
    }
    Assert.fail("bucket should have been removed");
  }

  @Test
  public void deleteNonExistingBucket() throws Exception {
    final String bucketName = "non-existing-bucket";

    try {
      // Delete a non-existing bucket should fail.
      deleteBucketRestCall(bucketName);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("delete a non-existing bucket should fail");
  }

  @Test
  public void deleteNonEmptyBucket() throws Exception {
    final String bucketName = "non-empty-bucket";

    createBucketRestCall(bucketName);

    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI fileUri = new AlluxioURI(uri.getPath() + "/file");
    mFileSystemMaster.createFile(fileUri, CreateFileOptions.defaults());

    // Verify the directory is created for the new bucket, and file is created under it.
    Assert.assertFalse(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());

    try {
      // Delete a non-empty bucket should fail.
      deleteBucketRestCall(bucketName);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("delete a non-empty bucket should fail");
  }

  private void putObjectTest(byte[] object) throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    createObjectRestCall(objectKey, object, null);

    // Verify the object is created for the new bucket.
    AlluxioURI bucketURI = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    AlluxioURI objectURI = new AlluxioURI(AlluxioURI.SEPARATOR + objectKey);
    List<FileInfo> fileInfos = mFileSystemMaster.listStatus(bucketURI,
        ListStatusOptions.defaults());
    Assert.assertEquals(1, fileInfos.size());
    Assert.assertEquals(objectURI.getPath(), fileInfos.get(0).getPath());

    // Verify the object's content.
    FileInStream is = mFileSystem.openFile(objectURI);
    byte[] writtenObjectContent = IOUtils.toString(is).getBytes();
    is.close();
    Assert.assertArrayEquals(object, writtenObjectContent);
  }

  @Test
  public void putSmallObject() throws Exception {
    putObjectTest("Hello World!".getBytes());
  }

  @Test
  public void putLargeObject() throws Exception {
    putObjectTest(CommonUtils.randomAlphaNumString(Constants.MB).getBytes());
  }

  @Test
  public void putObjectUnderNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    String message = "hello world";
    try {
      createObjectRestCall(objectKey, message.getBytes(), null);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("create object under non-existent bucket should fail");
  }

  @Test
  public void putObjectWithWrongMD5() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    String objectContent = "hello world";
    try {
      String wrongMD5 = BaseEncoding.base64().encode(objectContent.getBytes());
      createObjectRestCall(objectKey, objectContent.getBytes(), wrongMD5);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("create object with wrong Content-MD5 should fail");
  }

  private void getObjectTest(byte[] expectedObject) throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    createObjectRestCall(objectKey, expectedObject, null);
    Assert.assertArrayEquals(expectedObject, getObjectRestCall(objectKey).getBytes());
  }

  @Test
  public void getSmallObject() throws Exception {
    getObjectTest("Hello World!".getBytes());
  }

  @Test
  public void getLargeObject() throws Exception {
    getObjectTest(CommonUtils.randomAlphaNumString(Constants.MB).getBytes());
  }

  @Test
  public void getNonExistentObject() throws Exception {
    final String objectKey = "bucket/non-existent-object";
    try {
      getObjectRestCall(objectKey);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("get non-existent object should fail");
  }

  @Test
  public void getObjectMetadata() throws Exception {
    final String bucket = "bucket";
    createBucketRestCall(bucket);

    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object.txt";
    final byte[] objectContent = CommonUtils.randomAlphaNumString(10).getBytes();
    createObjectRestCall(objectKey, objectContent, null);

    HttpURLConnection connection = getObjectMetadataRestCall(objectKey);
    URIStatus status = mFileSystem.getStatus(
        new AlluxioURI(AlluxioURI.SEPARATOR + objectKey));
    // remove the milliseconds from the last modification time because the accuracy of HTTP dates
    // is up to seconds.
    long lastModified = status.getLastModificationTimeMs() / 1000 * 1000;
    Assert.assertEquals(lastModified, connection.getLastModified());
  }

  @Test
  public void getNonExistentObjectMetadata() throws Exception {
    final String objectKey = "bucket/non-existent-object";
    try {
      getObjectMetadataRestCall(objectKey);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("get metadata of non-existent object should fail");
  }

  @Test
  public void deleteObject() throws Exception {
    final String bucketName = "bucket-with-object-to-delete";
    createBucketRestCall(bucketName);

    final String objectName = "file";
    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI fileUri = new AlluxioURI(
        bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createFile(fileUri, CreateFileOptions.defaults());

    // Verify the directory is created for the new bucket, and file is created under it.
    Assert.assertFalse(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());

    deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);

    // Verify the object is deleted.
    Assert.assertTrue(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void deleteObjectAsAlluxioEmptyDir() throws Exception {
    final String bucketName = "bucket-with-empty-dir-to-delete";
    createBucketRestCall(bucketName);

    String objectName = "empty-dir/";
    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI dirUri = new AlluxioURI(
        bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createDirectory(dirUri, CreateDirectoryOptions.defaults());

    // Verify the directory is created for the new bucket, and empty-dir is created under it.
    Assert.assertFalse(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());

    deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);

    // Verify the empty-dir as a valid object is deleted.
    Assert.assertTrue(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void deleteObjectAsAlluxioNonEmptyDir() throws Exception {
    final String bucketName = "bucket-with-non-empty-dir-to-delete";
    createBucketRestCall(bucketName);

    String objectName = "non-empty-dir/";
    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    AlluxioURI dirUri = new AlluxioURI(
        bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createDirectory(dirUri, CreateDirectoryOptions.defaults());

    mFileSystemMaster.createFile(
        new AlluxioURI(dirUri.getPath() + "/file"), CreateFileOptions.defaults());

    Assert.assertFalse(
        mFileSystemMaster.listStatus(dirUri, ListStatusOptions.defaults()).isEmpty());

    try {
      deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("delete non-empty directory as an object should fail");
  }

  @Test
  public void deleteNonExistingObject() throws Exception {
    final String bucketName = "bucket-with-nothing";
    createBucketRestCall(bucketName);

    String objectName = "non-existing-object";
    try {
      deleteObjectRestCall(bucketName + AlluxioURI.SEPARATOR + objectName);
    } catch (AssertionError e) {
      // expected
      return;
    }
    Assert.fail("delete non-existing object should fail");
  }

  private void createBucketRestCall(String bucketName) throws Exception {
    String uri = S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName;
    new TestCase(mHostname, mPort, uri, NO_PARAMS, HttpMethod.PUT, null,
        TestCaseOptions.defaults()).run();
  }

  private HttpURLConnection deleteBucketRestCall(String bucketName) throws Exception {
    String uri = S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName;
    return new TestCase(mHostname, mPort, uri, NO_PARAMS, HttpMethod.DELETE, null,
        TestCaseOptions.defaults()).execute();
  }

  private void createObjectRestCall(String objectKey, byte[] objectContent, String md5)
      throws Exception {
    String uri = S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + objectKey;
    TestCaseOptions options = TestCaseOptions.defaults();
    if (md5 == null) {
      MessageDigest md5Hash = MessageDigest.getInstance("MD5");
      byte[] md5Digest = md5Hash.digest(objectContent);
      md5 = BaseEncoding.base64().encode(md5Digest);
    }
    options.setMD5(md5);
    options.setInputStream(new ByteArrayInputStream(objectContent));
    new TestCase(mHostname, mPort, uri, NO_PARAMS, HttpMethod.PUT, null, options)
        .run();
  }

  private HttpURLConnection getObjectMetadataRestCall(String objectKey) throws Exception {
    String uri = S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + objectKey;
    return new TestCase(mHostname, mPort, uri, NO_PARAMS, HttpMethod.HEAD, null,
        TestCaseOptions.defaults()).execute();
  }

  private String getObjectRestCall(String objectKey) throws Exception {
    String uri = S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + objectKey;
    return new TestCase(mHostname, mPort, uri, NO_PARAMS, HttpMethod.GET, null,
        TestCaseOptions.defaults()).call();
  }

  private void deleteObjectRestCall(String objectKey) throws Exception {
    String uri = S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + objectKey;
    new TestCase(mHostname, mPort, uri, NO_PARAMS, HttpMethod.DELETE, null,
        TestCaseOptions.defaults()).run();
  }
}
