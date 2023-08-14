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

package alluxio.underfs.gcs;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.OpenOptions;

import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.model.StorageObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Unit tests for the {@link GCSUnderFileSystem}.
 */
public class GCSUnderFileSystemTest {
  private GCSUnderFileSystem mGCSUnderFileSystem;
  private GoogleStorageService mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";
  private static final String KEY = "key";

  private static final String BUCKET_NAME = "bucket";

  /**
   * Set up.
   */
  @Before
  public void before() {
    mClient = mock(GoogleStorageService.class);

    mGCSUnderFileSystem = new GCSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
  }

  /**
   * Test case for {@link GCSUnderFileSystem#getUnderFSType()}.
   */
  @Test
  public void getUnderFSType() {
    Assert.assertEquals("gcs", mGCSUnderFileSystem.getUnderFSType());
  }

  /**
   * Test case for {@link GCSUnderFileSystem#copyObject(String, String)}.
   */
  @Test
  public void testCopyObject() throws ServiceException {
    // test successful copy object
    when(mClient.copyObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.any(StorageObject.class),
        ArgumentMatchers.anyBoolean())).thenReturn(null);
    boolean result = mGCSUnderFileSystem.copyObject(SRC, DST);
    Assert.assertTrue(result);

    // test copy object exception
    Mockito.when(mClient.copyObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.any(StorageObject.class),
        ArgumentMatchers.anyBoolean())).thenThrow(ServiceException.class);
    try {
      mGCSUnderFileSystem.copyObject(SRC, DST);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ServiceException);
    }
  }

  /**
   * Test case for {@link GCSUnderFileSystem#createEmptyObject(String)}.
   */
  @Test
  public void testCreateEmptyObject() throws ServiceException {

    // test successful create empty object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(),
            ArgumentMatchers.any(GSObject.class))).thenReturn(null);
    boolean result = mGCSUnderFileSystem.createEmptyObject(KEY);
    Assert.assertTrue(result);

    // test create empty object exception
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(),
            ArgumentMatchers.any(GSObject.class))).thenThrow(ServiceException.class);
    try {
      mGCSUnderFileSystem.createEmptyObject(KEY);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ServiceException);
    }
  }

  /**
   * Test case for {@link GCSUnderFileSystem#createObject(String)}.
   */
  @Test
  public void testCreateObject() throws IOException, ServiceException {
    // test successful create object
    Mockito.when(mClient.putObject(ArgumentMatchers.anyString(),
        ArgumentMatchers.any(GSObject.class))).thenReturn(null);
    OutputStream result = mGCSUnderFileSystem.createObject(KEY);
    Assert.assertTrue(result instanceof GCSOutputStream);
  }

  /**
   * Test case for {@link GCSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException, ServiceException {
    when(mClient.listObjectsChunked(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
        .thenThrow(ServiceException.class);

    boolean result = mGCSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    assertFalse(result);
  }

  /**
   * Test case for {@link GCSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws IOException, ServiceException {
    when(mClient.listObjectsChunked(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
        .thenThrow(ServiceException.class);

    boolean result = mGCSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    assertFalse(result);
  }

  /**
   * Test case for {@link GCSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException, ServiceException {
    when(mClient.listObjectsChunked(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
        .thenThrow(ServiceException.class);

    boolean result = mGCSUnderFileSystem.renameFile(SRC, DST);
    assertFalse(result);
  }

  /**
   * Test case for {@link GCSUnderFileSystem#getFolderSuffix()}.
   */
  @Test
  public void testGetFolderSuffix() {
    Assert.assertEquals("/", mGCSUnderFileSystem.getFolderSuffix());
  }

  /**
   * Test case for {@link GCSUnderFileSystem#getObjectListingChunk(String, String, String)}.
   */
  @Test
  public void testGetObjectListingChunk() throws ServiceException {
    // test successful get object listing chunk
    Mockito.when(mClient.listObjectsChunked(ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyString())).thenReturn(new StorageObjectsChunk(
                null, null, null, null, null));
    String delimiter = "/";
    String prefix = "";
    Object result = mGCSUnderFileSystem.getObjectListingChunk(KEY, delimiter, prefix);
    Assert.assertTrue(result instanceof StorageObjectsChunk);
  }

  /**
   * Test case for {@link GCSUnderFileSystem#isDirectory(String)}.
   */
  @Test
  public void testIsDirectory() throws IOException {
    Assert.assertTrue(mGCSUnderFileSystem.isDirectory("/"));
  }

  /**
   * Test case for {@link GCSUnderFileSystem#openPositionRead(String, long)}.
   */
  @Test
  public void testOpenPositionRead() {
    PositionReader result = mGCSUnderFileSystem.openPositionRead(KEY, 1L);
    Assert.assertTrue(result instanceof GCSPositionReader);
  }

  /**
   * Test case for {@link GCSUnderFileSystem#getRootKey()}.
   */
  @Test
  public void testGetRootKey() {
    Assert.assertEquals(Constants.HEADER_GCS + BUCKET_NAME, mGCSUnderFileSystem.getRootKey());
  }

  /**
   * Test case for {@link GCSUnderFileSystem#openObject(String, OpenOptions, RetryPolicy)}.
   */
  @Test
  public void testOpenObject() throws IOException, ServiceException {
    // test successful open object
    Mockito.when(mClient.getObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(new GSObject());
    OpenOptions options = OpenOptions.defaults();
    RetryPolicy retryPolicy = new CountingRetry(1);
    InputStream result = mGCSUnderFileSystem.openObject(KEY, options, retryPolicy);
    Assert.assertTrue(result instanceof GCSInputStream);
  }
}
