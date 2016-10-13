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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.ListObjectsRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for the {@link OSSUnderFileSystem}.
 */
public class OSSUnderFileSystemTest {

  private OSSUnderFileSystem mOSSUnderFileSystem;
  private OSSClient mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final String BUCKET_PREFIX = "prefix";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, ServiceException {
    mClient = Mockito.mock(OSSClient.class);

    mOSSUnderFileSystem = new OSSUnderFileSystem(new AlluxioURI(""), mClient,
        BUCKET_NAME, BUCKET_PREFIX);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#delete(String, boolean)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.delete(PATH, false);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#delete(String, boolean)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.delete(PATH, true);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OSSUnderFileSystem#rename(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.rename(SRC, DST);
    Assert.assertFalse(result);
  }
}
