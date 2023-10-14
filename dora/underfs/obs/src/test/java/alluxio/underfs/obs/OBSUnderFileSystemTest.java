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

package alluxio.underfs.obs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.OpenOptions;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.HeaderResponse;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Unit tests for the {@link OBSUnderFileSystem}.
 */
public class OBSUnderFileSystemTest {

  private OBSUnderFileSystem mOBSUnderFileSystem;
  private ObsClient mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";
  private static final String KEY = "key";

  private static final String BUCKET_NAME = "bucket";
  private static final String BUCKET_TYPE = "obs";

  /**
   * Set up.
   */
  @Before
  public void before() throws ObsException {
    mClient = Mockito.mock(ObsClient.class);
    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        BUCKET_TYPE, UnderFileSystemConfiguration.defaults(Configuration.global()));
  }

  /**
   * Test case for {@link OBSUnderFileSystem#getUnderFSType()}.
   */
  @Test
  public void getUnderFSType() {
    Assert.assertEquals("obs", mOBSUnderFileSystem.getUnderFSType());
  }

  /**
   * Test case for {@link OBSUnderFileSystem#copyObject(String, String)}.
   */
  @Test
  public void testCopyObject() {
    // test successful copy object
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(null);
    boolean result = mOBSUnderFileSystem.copyObject(SRC, DST);
    Assert.assertTrue(result);

    // test copy object exception
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString())).thenThrow(ObsException.class);
    try {
      mOBSUnderFileSystem.copyObject(SRC, DST);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ObsException);
    }
  }

  /**
   * Test case for {@link OBSUnderFileSystem#createEmptyObject(String)}.
   */
  @Test
  public void testCreateEmptyObject() {
    // test successful create empty object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.any(InputStream.class), ArgumentMatchers.any(ObjectMetadata.class)))
        .thenReturn(null);
    boolean result = mOBSUnderFileSystem.createEmptyObject(KEY);
    Assert.assertTrue(result);

    // test create empty object exception
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.any(InputStream.class), ArgumentMatchers.any(ObjectMetadata.class)))
        .thenThrow(ObsException.class);
    try {
      mOBSUnderFileSystem.createEmptyObject(KEY);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ObsException);
    }
  }

  /**
   * Test case for {@link OBSUnderFileSystem#createObject(String)}.
   */
  @Test
  public void testCreateObject() throws IOException {
    // test successful create object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.any(InputStream.class),
        ArgumentMatchers.any(ObjectMetadata.class))).thenReturn(null);
    OutputStream result = mOBSUnderFileSystem.createObject(KEY);
    Assert.assertTrue(result instanceof OBSOutputStream);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#deleteObjects(List)}.
   */
  @Test
  public void testDeleteObjects() throws IOException {
    String[] stringKeys = new String[]{"key1", "key2", "key3"};
    List<String> keys = new ArrayList<>();
    Collections.addAll(keys, stringKeys);
    List<DeleteObjectsResult.DeleteObjectResult> deletedObjects =
        new ArrayList<>();
    for (String key : keys) {
      DeleteObjectsResult.DeleteObjectResult deletedObject =
          new DeleteObjectsResult.DeleteObjectResult(key, "", true, "");
      deletedObjects.add(deletedObject);
    }
    // test successful delete objects
    Mockito.when(mClient.deleteObjects(ArgumentMatchers.any(DeleteObjectsRequest.class)))
        .thenReturn(new DeleteObjectsResult(deletedObjects, null));

    List<String> result = mOBSUnderFileSystem.deleteObjects(keys);
    Assert.assertEquals(keys, result);

    // test delete objects exception
    Mockito.when(mClient.deleteObjects(ArgumentMatchers.any(DeleteObjectsRequest.class)))
        .thenThrow(ObsException.class);
    try {
      mOBSUnderFileSystem.deleteObjects(keys);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }
  }

  /**
   * Test case for {@link OBSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ObsException.class);

    boolean result = mOBSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ObsException.class);

    boolean result = mOBSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    System.out.println(result);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ObsException.class);

    boolean result = mOBSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }

  @Test
  public void judgeDirectoryInBucket() throws Exception {
    ObjectMetadata fileMeta = new ObjectMetadata();
    fileMeta.setLastModified(new Date());
    fileMeta.getMetadata().put("mode", 33152);
    fileMeta.setContentLength(10L);
    ObjectMetadata dirMeta = new ObjectMetadata();
    dirMeta.setLastModified(new Date());
    dirMeta.getMetadata().put("mode", 16877);
    dirMeta.setContentLength(0L);
    /**
     * /xx/file1/ ( File1 actually exists, which is a file) , there is / after file1 name.
     * When OBS, the path object meta is null.
     * When PFS, the path object meta is not null. The object meta is same as /xx/file1
     */
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "pfs_file1"))
        .thenReturn(fileMeta);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "pfs_file1/"))
        .thenReturn(fileMeta);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "obs_file1"))
        .thenReturn(fileMeta);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "obs_file1/"))
        .thenReturn(null);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "dir1"))
        .thenReturn(dirMeta);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "dir1/"))
        .thenReturn(dirMeta);

    // PFS Bucket
    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, "pfs",
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    Assert.assertNotNull(mOBSUnderFileSystem.getObjectStatus("pfs_file1"));
    Assert.assertNull(mOBSUnderFileSystem.getObjectStatus("pfs_file1/"));
    Assert.assertNull(mOBSUnderFileSystem.getObjectStatus("dir1"));
    Assert.assertTrue(mOBSUnderFileSystem.isDirectory("dir1"));
    Assert.assertFalse(mOBSUnderFileSystem.isDirectory("pfs_file1"));

    // OBS Bucket
    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, "obs",
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "dir1"))
        .thenReturn(null);
    Assert.assertNotNull(mOBSUnderFileSystem.getObjectStatus("obs_file1"));
    Assert.assertNull(mOBSUnderFileSystem.getObjectStatus("obs_file1/"));
    Assert.assertNull(mOBSUnderFileSystem.getObjectStatus("dir1"));
    Assert.assertTrue(mOBSUnderFileSystem.isDirectory("dir1"));
    Assert.assertFalse(mOBSUnderFileSystem.isDirectory("obs_file1"));
  }

  @Test
  public void nullObjectMetaTest() throws Exception {
    ObjectMetadata fileMeta = new ObjectMetadata();
    fileMeta.setContentLength(10L);
    ObjectMetadata dirMeta = new ObjectMetadata();
    dirMeta.setContentLength(0L);
    /**
     * /xx/file1/ ( File1 actually exists, which is a file) , there is / after file1 name.
     * When OBS, the path object meta is null.
     * When PFS, the path object meta is not null. The object meta is same as /xx/file1
     */
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "pfs_file1"))
        .thenReturn(fileMeta);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "dir1"))
        .thenReturn(dirMeta);

    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, "obs",
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    Assert.assertNotNull(mOBSUnderFileSystem.getObjectStatus("pfs_file1"));
    Assert.assertNotNull(mOBSUnderFileSystem.getObjectStatus("dir1"));
  }

  /**
   * Test case for {@link OBSUnderFileSystem#getFolderSuffix()}.
   */
  @Test
  public void testGetFolderSuffix() {
    Assert.assertEquals("/", mOBSUnderFileSystem.getFolderSuffix());
  }

  /**
   * Test case for {@link OBSUnderFileSystem#getObjectListingChunk(ListObjectsRequest)}.
   */
  @Test
  public void testGetObjectListingChunk() {
    // test successful get object listing chunk
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenReturn(new ObjectListing(null, null, null, false,
            null, null, 0, null, null, null));
    ListObjectsRequest request = new ListObjectsRequest();
    HeaderResponse result = mOBSUnderFileSystem.getObjectListingChunk(request);
    Assert.assertTrue(result instanceof ObjectListing);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#isDirectory(String)}.
   */
  @Test
  public void testIsDirectory() throws IOException {
    Assert.assertTrue(mOBSUnderFileSystem.isDirectory("/"));
  }

  /**
   * Test case for {@link OBSUnderFileSystem#openPositionRead(String, long)}.
   */
  @Test
  public void testOpenPositionRead() {
    PositionReader result = mOBSUnderFileSystem.openPositionRead(KEY, 1L);
    Assert.assertTrue(result instanceof OBSPositionReader);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#getRootKey()}.
   */
  @Test
  public void testGetRootKey() {
    Assert.assertEquals(Constants.HEADER_OBS + BUCKET_NAME, mOBSUnderFileSystem.getRootKey());
  }

  /**
   * Test case for {@link OBSUnderFileSystem#openObject(String, OpenOptions, RetryPolicy)}.
   */
  @Test
  public void testOpenObject() throws IOException {
    // test successful open object
    Mockito.when(mClient.getObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(new ObsObject());
    OpenOptions options = OpenOptions.defaults();
    RetryPolicy retryPolicy = new CountingRetry(1);
    InputStream result = mOBSUnderFileSystem.openObject(KEY, options, retryPolicy);
    Assert.assertTrue(result instanceof OBSInputStream);
  }
}
