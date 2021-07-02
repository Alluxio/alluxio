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
import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

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

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, ServiceException {
    mClient = Mockito.mock(OSSClient.class);

    mOSSUnderFileSystem = new OSSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults()));
  }

  /**
   * Test case for {@link OSSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
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
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
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
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ServiceException.class);

    boolean result = mOSSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }
}
