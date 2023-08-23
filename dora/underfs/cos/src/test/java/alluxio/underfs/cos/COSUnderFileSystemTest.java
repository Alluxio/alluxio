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

package alluxio.underfs.cos;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.OpenOptions;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.DeleteObjectsResult;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the {@link COSUnderFileSystem}.
 */
public class COSUnderFileSystemTest {

  private COSUnderFileSystem mCOSUnderFileSystem;
  private COSClient mClient;
  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";
  private static final String KEY = "key";

  private static final String BUCKET_NAME = "bucket";
  private static final String APP_ID = "appId";

  /**
   * Set up.
   */
  @Before
  public void before() throws IOException {
    mClient = Mockito.mock(COSClient.class);

    mCOSUnderFileSystem = new COSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, APP_ID,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
  }

  /**
   * Test case for {@link COSUnderFileSystem#getUnderFSType()}.
   */
  @Test
  public void getUnderFSType() {
    Assert.assertEquals("cos", mCOSUnderFileSystem.getUnderFSType());
  }

  /**
   * Test case for {@link COSUnderFileSystem#copyObject(String, String)}.
   */
  @Test
  public void testCopyObject() {
    // test successful copy object
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(null);
    boolean result = mCOSUnderFileSystem.copyObject(SRC, DST);
    Assert.assertTrue(result);

    // test copy object exception
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString())).thenThrow(CosClientException.class);
    try {
      mCOSUnderFileSystem.copyObject(SRC, DST);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof CosClientException);
    }
  }

  /**
   * Test case for {@link COSUnderFileSystem#createEmptyObject(String)}.
   */
  @Test
  public void testCreateEmptyObject() {
    // test successful create empty object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.any(InputStream.class), ArgumentMatchers.any(ObjectMetadata.class)))
        .thenReturn(null);
    boolean result = mCOSUnderFileSystem.createEmptyObject(KEY);
    Assert.assertTrue(result);

    // test create empty object exception
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.any(InputStream.class), ArgumentMatchers.any(ObjectMetadata.class)))
        .thenThrow(CosClientException.class);
    try {
      mCOSUnderFileSystem.createEmptyObject(KEY);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof CosClientException);
    }
  }

  /**
   * Test case for {@link COSUnderFileSystem#createObject(String)}.
   */
  @Test
  public void testCreateObject() throws IOException {
    // test successful create object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.any(InputStream.class),
        ArgumentMatchers.any(ObjectMetadata.class))).thenReturn(null);
    OutputStream result = mCOSUnderFileSystem.createObject(KEY);
    Assert.assertTrue(result instanceof COSOutputStream);
  }

  /**
   * Test case for {@link COSUnderFileSystem#deleteObjects(List)}.
   */
  @Test
  public void testDeleteObjects() throws IOException {
    String[] stringKeys = new String[]{"key1", "key2", "key3"};
    List<String> keys = new ArrayList<>();
    Collections.addAll(keys, stringKeys);
    List<DeleteObjectsResult.DeletedObject> deletedObjects =
        new ArrayList<>();
    for (String key : keys) {
      DeleteObjectsResult.DeletedObject deletedObject =
          new DeleteObjectsResult.DeletedObject();
      deletedObject.setKey(key);
      deletedObjects.add(deletedObject);
    }
    // test successful delete objects
    Mockito.when(mClient.deleteObjects(ArgumentMatchers.any(DeleteObjectsRequest.class)))
        .thenReturn(new DeleteObjectsResult(deletedObjects));

    List<String> result = mCOSUnderFileSystem.deleteObjects(keys);
    Assert.assertEquals(keys, result);

    // test delete objects exception
    Mockito.when(mClient.deleteObjects(ArgumentMatchers.any(DeleteObjectsRequest.class)))
        .thenThrow(CosClientException.class);
    try {
      mCOSUnderFileSystem.deleteObjects(keys);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }
  }

  /**
   * Test case for {@link COSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnClientException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(CosClientException.class);

    boolean result = mCOSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link COSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnClientException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(CosClientException.class);

    boolean result = mCOSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link COSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnClientException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(CosClientException.class);

    boolean result = mCOSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link COSUnderFileSystem#getFolderSuffix()}.
   */
  @Test
  public void testGetFolderSuffix() {
    Assert.assertEquals("/", mCOSUnderFileSystem.getFolderSuffix());
  }

  /**
   * Test case for {@link COSUnderFileSystem#getObjectListingChunk(ListObjectsRequest)}.
   */
  @Test
  public void testGetObjectListingChunk() {
    // test successful get object listing chunk
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenReturn(new ObjectListing());
    ListObjectsRequest request = new ListObjectsRequest();
    Serializable result = mCOSUnderFileSystem.getObjectListingChunk(request);
    Assert.assertTrue(result instanceof ObjectListing);
  }

  /**
   * Test case for {@link COSUnderFileSystem#isDirectory(String)}.
   */
  @Test
  public void testIsDirectory() throws IOException {
    Assert.assertTrue(mCOSUnderFileSystem.isDirectory("/"));
  }

  /**
   * Test case for {@link COSUnderFileSystem#openPositionRead(String, long)}.
   */
  @Test
  public void testOpenPositionRead() {
    PositionReader result = mCOSUnderFileSystem.openPositionRead(KEY, 1L);
    Assert.assertTrue(result instanceof COSPositionReader);
  }

  /**
   * Test case for {@link COSUnderFileSystem#getRootKey()}.
   */
  @Test
  public void testGetRootKey() {
    Assert.assertEquals(Constants.HEADER_COS + BUCKET_NAME, mCOSUnderFileSystem.getRootKey());
  }

  /**
   * Test case for {@link COSUnderFileSystem#openObject(String, OpenOptions, RetryPolicy)}.
   */
  @Test
  public void testOpenObject() throws IOException {
    // test successful open object
    Mockito.when(mClient.getObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(new COSObject());
    OpenOptions options = OpenOptions.defaults();
    RetryPolicy retryPolicy = new CountingRetry(1);
    InputStream result = mCOSUnderFileSystem.openObject(KEY, options, retryPolicy);
    Assert.assertTrue(result instanceof COSInputStream);
  }
}
