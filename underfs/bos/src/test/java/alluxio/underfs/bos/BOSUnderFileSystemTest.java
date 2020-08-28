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

package alluxio.underfs.bos;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import com.baidubce.services.bos.BosClient;
import com.baidubce.BceServiceException;
import com.baidubce.services.bos.model.ListObjectsRequest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for the {@link BOSUnderFileSystem}.
 */
public class BOSUnderFileSystemTest {

  private BOSUnderFileSystem mBOSUnderFileSystem;
  private BosClient mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, BceServiceException {
    mClient = Mockito.mock(BosClient.class);

    mBOSUnderFileSystem = new BOSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults()));
  }

  /**
   * Test case for {@link BOSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnBceServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(BceServiceException.class);

    boolean result = mBOSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link BOSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnBceServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(BceServiceException.class);

    boolean result = mBOSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link BOSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnBceServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(BceServiceException.class);

    boolean result = mBOSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }
}
