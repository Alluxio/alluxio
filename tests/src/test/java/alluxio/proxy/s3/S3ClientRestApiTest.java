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
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.rest.TestCaseOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link S3RestServiceHandler}.
 */
public final class S3ClientRestApiTest extends RestApiTest {
  private static final alluxio.master.file.options.GetStatusOptions GET_STATUS_OPTIONS =
      alluxio.master.file.options.GetStatusOptions.defaults();
  private static final Map<String, String> NO_PARAMS = new HashMap<>();

  private static final String S3_SERVICE_PREFIX = "s3";
  private static final String BUCKET_SEPARATOR = ":";

  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getProxyProcess().getWebLocalPort();
    mFileSystemMaster = mResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  @Test
  public void putBucket() throws Exception {
    final String bucket = "bucket";
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucket, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();
    // Verify the directory is created for the new bucket.
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
    AlluxioURI uri = new AlluxioURI(
        AlluxioURI.SEPARATOR + mountPoint + AlluxioURI.SEPARATOR + bucketName);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + s3Path, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    // Verify the directory is created for the new bucket, under the mount point.
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void putBucketUnderNestedMountPoint() throws Exception {
    final String mountPointParent = "mounts";
    final String mountPointName = "s3";
    final String bucketName = "bucket";
    final String s3Path =
        mountPointParent + BUCKET_SEPARATOR + mountPointName + BUCKET_SEPARATOR + bucketName;

    mFileSystemMaster.createDirectory(new AlluxioURI(AlluxioURI.SEPARATOR + mountPointParent),
        CreateDirectoryOptions.defaults());
    AlluxioURI mountPointPath = new AlluxioURI(AlluxioURI.SEPARATOR + mountPointParent
        + AlluxioURI.SEPARATOR + mountPointName);
    mFileSystemMaster.mount(mountPointPath, new AlluxioURI(mFolder.newFolder().getAbsolutePath()),
        MountOptions.defaults());

    // Create a new bucket under an existing nested mount point.
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + mountPointParent + AlluxioURI.SEPARATOR
        + mountPointName + AlluxioURI.SEPARATOR + bucketName);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + s3Path, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    // Verify the directory is created for the new bucket, under the mount point.
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void putBucketUnderNonExistingMountPoint() throws Exception {
    final String mountPoint = "s3";
    final String bucketName = "bucket";
    final String s3Path = mountPoint + BUCKET_SEPARATOR + bucketName;

    try {
      // Create a new bucket under a non-existing mount point should fail.
      new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + s3Path, NO_PARAMS,
          HttpMethod.PUT, null, TestCaseOptions.defaults()).run();
      Assert.fail();
    } catch (AssertionError e) {
      // expected
    }
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
      new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + s3Path, NO_PARAMS,
          HttpMethod.PUT, null, TestCaseOptions.defaults()).run();
      Assert.fail();
    } catch (AssertionError e) {
      // expected
    }
  }

  @Test
  public void deleteBucket() throws Exception {
    final String bucket = "bucket-to-delete";
    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucket);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucket, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();
    // Verify the directory is created for the new bucket.
    Assert.assertTrue(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());

    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucket, NO_PARAMS,
        HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();
    try {
      mFileSystemMaster.getFileInfo(uri, GET_STATUS_OPTIONS);
      Assert.fail("bucket should have been removed");
    } catch (FileDoesNotExistException e) {
      // expected
    }
  }

  @Test
  public void deleteNonExistingBucket() throws Exception {
    final String bucketName = "non-existing-bucket";

    try {
      // Delete a non-existing bucket should fail.
      new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName,
          NO_PARAMS, HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();
      Assert.fail("delete a non-existing bucket should fail");
    } catch (AssertionError e) {
      // expected
    }
  }

  @Test
  public void deleteNonEmptyBucket() throws Exception {
    final String bucketName = "non-empty-bucket";

    AlluxioURI uri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    AlluxioURI fileUri = new AlluxioURI(uri.getPath() + "/file");
    mFileSystemMaster.createFile(fileUri, CreateFileOptions.defaults());

    // Verify the directory is created for the new bucket, and file is created under it.
    Assert.assertFalse(mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults()).isEmpty());

    try {
      // Delete a non-empty bucket should fail.
      new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName,
          NO_PARAMS, HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();
      Assert.fail("delete a non-empty bucket should fail");
    } catch (AssertionError e) {
      // expected
    }
  }

  @Test
  public void deleteObject() throws Exception {
    final String bucketName = "bucket-with-object-to-delete";

    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    final String objectName = "file";
    AlluxioURI fileUri = new AlluxioURI(bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createFile(fileUri, CreateFileOptions.defaults());

    // Verify the directory is created for the new bucket, and file is created under it.
    Assert.assertFalse(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());

    new TestCase(mHostname, mPort,
        S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName + AlluxioURI.SEPARATOR + objectName,
        NO_PARAMS, HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();

    // Verify the object is deleted.
    Assert.assertTrue(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void deleteObjectAsAlluxioEmptyDir() throws Exception {
    final String bucketName = "bucket-with-empty-dir-to-delete";

    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    String objectName = "empty-dir/";
    AlluxioURI dirUri = new AlluxioURI(bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createDirectory(dirUri, CreateDirectoryOptions.defaults());

    // Verify the directory is created for the new bucket, and empty-dir is created under it.
    Assert.assertFalse(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());

    new TestCase(mHostname, mPort,
        S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName + AlluxioURI.SEPARATOR + objectName,
        NO_PARAMS, HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();

    // Verify the empty-dir as a valid object is deleted.
    Assert.assertTrue(
        mFileSystemMaster.listStatus(bucketUri, ListStatusOptions.defaults()).isEmpty());
  }

  @Test
  public void deleteObjectAsAlluxioNonEmptyDir() throws Exception {
    final String bucketName = "bucket-with-non-empty-dir-to-delete";

    AlluxioURI bucketUri = new AlluxioURI(AlluxioURI.SEPARATOR + bucketName);
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    String objectName = "non-empty-dir/";
    AlluxioURI dirUri = new AlluxioURI(bucketUri.getPath() + AlluxioURI.SEPARATOR + objectName);
    mFileSystemMaster.createDirectory(dirUri, CreateDirectoryOptions.defaults());

    mFileSystemMaster.createFile(
        new AlluxioURI(dirUri.getPath() + "/file"), CreateFileOptions.defaults());

    Assert.assertFalse(
        mFileSystemMaster.listStatus(dirUri, ListStatusOptions.defaults()).isEmpty());

    try {
      new TestCase(mHostname, mPort,
          S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName + AlluxioURI.SEPARATOR + objectName,
          NO_PARAMS, HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();
    } catch (AssertionError e) {
      // expected
    }
  }

  @Test
  public void deleteNonExistingObject() throws Exception {
    final String bucketName = "bucket-with-nothing";
    new TestCase(mHostname, mPort, S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName, NO_PARAMS,
        HttpMethod.PUT, null, TestCaseOptions.defaults()).run();

    String objectName = "non-existing-object";
    try {
      new TestCase(mHostname, mPort,
          S3_SERVICE_PREFIX + AlluxioURI.SEPARATOR + bucketName + AlluxioURI.SEPARATOR + objectName,
          NO_PARAMS, HttpMethod.DELETE, null, TestCaseOptions.defaults()).run();
    } catch (AssertionError e) {
      // expected
    }
  }
}
