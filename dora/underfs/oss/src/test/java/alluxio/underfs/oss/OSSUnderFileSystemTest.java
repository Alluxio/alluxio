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

package alluxio.underfs.oss;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.OpenOptions;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.GenericResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
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
import java.util.List;

/**
 * Unit tests for the {@link OSSUnderFileSystem}.
 */
public class OSSUnderFileSystemTest {

  private OSSUnderFileSystem mOSSUnderFileSystem;
  private OSSClient mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";
  private static final String KEY = "key";

  private static final String BUCKET_NAME = "bucket";

  /**
   * Set up.
   */
  @Before
  public void before() throws ServiceException {
    mClient = Mockito.mock(OSSClient.class);

    mOSSUnderFileSystem = new OSSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
  }

  /**
   * Test case for {@link OSSUnderFileSystem#getUnderFSType()}.
   */
  @Test
  public void getUnderFSType() {
    Assert.assertEquals("oss", mOSSUnderFileSystem.getUnderFSType());
  }

  /**
   * Test case for {@link OSSUnderFileSystem#copyObject(String, String)}.
   */
  @Test
  public void testCopyObject() {
    // test successful copy object
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(null);
    boolean result = mOSSUnderFileSystem.copyObject(SRC, DST);
    Assert.assertTrue(result);

    // test copy object exception
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString())).thenThrow(ServiceException.class);
    try {
      mOSSUnderFileSystem.copyObject(SRC, DST);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ServiceException);
    }
  }

  /**
   * Test case for {@link OSSUnderFileSystem#createEmptyObject(String)}.
   */
  @Test
  public void testCreateEmptyObject() {
    // test successful create empty object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.any(InputStream.class), ArgumentMatchers.any(ObjectMetadata.class)))
        .thenReturn(null);
    boolean result = mOSSUnderFileSystem.createEmptyObject(KEY);
    Assert.assertTrue(result);

    // test create empty object exception
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.any(InputStream.class), ArgumentMatchers.any(ObjectMetadata.class)))
        .thenThrow(ServiceException.class);
    try {
      mOSSUnderFileSystem.createEmptyObject(KEY);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ServiceException);
    }
  }

  /**
   * Test case for {@link OSSUnderFileSystem#createObject(String)}.
   */
  @Test
  public void testCreateObject() throws IOException {
    // test successful create object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.any(InputStream.class),
        ArgumentMatchers.any(ObjectMetadata.class))).thenReturn(null);
    OutputStream result = mOSSUnderFileSystem.createObject(KEY);
    Assert.assertTrue(result instanceof OSSOutputStream);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#deleteObjects(List)}.
   */
  @Test
  public void testDeleteObjects() throws IOException {
    String[] stringKeys = new String[]{"key1", "key2", "key3"};
    List<String> keys = new ArrayList<>();
    Collections.addAll(keys, stringKeys);
    // test successful delete objects
    Mockito.when(mClient.deleteObjects(ArgumentMatchers.any(DeleteObjectsRequest.class)))
        .thenReturn(new DeleteObjectsResult(keys));

    List<String> result = mOSSUnderFileSystem.deleteObjects(keys);
    Assert.assertEquals(keys, result);

    // test delete objects exception
    Mockito.when(mClient.deleteObjects(ArgumentMatchers.any(DeleteObjectsRequest.class)))
        .thenThrow(ServiceException.class);
    try {
      mOSSUnderFileSystem.deleteObjects(keys);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }
  }

  /**
   * Test case for {@link OSSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnClientException() throws IOException {
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#getFolderSuffix()}.
   */
  @Test
  public void testGetFolderSuffix() {
    Assert.assertEquals("/", mOSSUnderFileSystem.getFolderSuffix());
  }

  /**
   * Test case for {@link OSSUnderFileSystem#getObjectListingChunk(ListObjectsRequest)}.
   */
  @Test
  public void testGetObjectListingChunk() {
    // test successful get object listing chunk
    Mockito.when(mClient.listObjects(ArgumentMatchers.any(ListObjectsRequest.class)))
        .thenReturn(new ObjectListing());
    ListObjectsRequest request = new ListObjectsRequest();
    GenericResult result = mOSSUnderFileSystem.getObjectListingChunk(request);
    Assert.assertTrue(result instanceof ObjectListing);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#isDirectory(String)}.
   */
  @Test
  public void testIsDirectory() throws IOException {
    Assert.assertTrue(mOSSUnderFileSystem.isDirectory("/"));
  }

  /**
   * Test case for {@link OSSUnderFileSystem#openPositionRead(String, long)}.
   */
  @Test
  public void testOpenPositionRead() {
    PositionReader result = mOSSUnderFileSystem.openPositionRead(KEY, 1L);
    Assert.assertTrue(result instanceof OSSPositionReader);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#getRootKey()}.
   */
  @Test
  public void testGetRootKey() {
    Assert.assertEquals(Constants.HEADER_OSS + BUCKET_NAME, mOSSUnderFileSystem.getRootKey());
  }

  /**
   * Test case for {@link OSSUnderFileSystem#openObject(String, OpenOptions, RetryPolicy)}.
   */
  @Test
  public void testOpenObject() throws IOException {
    // test successful open object
    Mockito.when(mClient.getObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(new OSSObject());
    OpenOptions options = OpenOptions.defaults();
    RetryPolicy retryPolicy = new CountingRetry(1);
    InputStream result = mOSSUnderFileSystem.openObject(KEY, options, retryPolicy);
    Assert.assertTrue(result instanceof OSSInputStream);
  }
}
