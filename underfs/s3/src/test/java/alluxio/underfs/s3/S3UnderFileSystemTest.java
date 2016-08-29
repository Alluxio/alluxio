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

package alluxio.underfs.s3;

import alluxio.AlluxioURI;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for the {@link S3UnderFileSystem}.
 */
public class S3UnderFileSystemTest {

  private S3UnderFileSystem mS3UnderFileSystem;
  private S3Service mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final String BUCKET_PREFIX = "prefix";
  private static final short BUCKET_MODE = 0;
  private static final String ACCOUNT_OWNER = "account owner";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, ServiceException {
    mClient = Mockito.mock(S3Service.class);

    mS3UnderFileSystem = new S3UnderFileSystem(new AlluxioURI(""),
        mClient, BUCKET_NAME, BUCKET_PREFIX, BUCKET_MODE, ACCOUNT_OWNER);
  }

  /**
   * Test case for {@link S3UnderFileSystem#delete(String, boolean)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException, ServiceException {
    Mockito.when(mClient.listObjectsChunked(Matchers.anyString(), Matchers.anyString(),
        Matchers.anyString(), Matchers.anyLong(), Matchers.anyString()))
        .thenThrow(ServiceException.class);

    boolean result = mS3UnderFileSystem.delete(PATH, false);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link S3UnderFileSystem#delete(String, boolean)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws IOException, ServiceException {
    Mockito.when(mClient.listObjectsChunked(Matchers.anyString(), Matchers.anyString(),
        Matchers.anyString(), Matchers.anyLong(), Matchers.anyString()))
        .thenThrow(ServiceException.class);

    boolean result = mS3UnderFileSystem.delete(PATH, true);
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link S3UnderFileSystem#rename(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException, ServiceException {
    Mockito.when(mClient.listObjectsChunked(Matchers.anyString(), Matchers.anyString(),
        Matchers.anyString(), Matchers.anyLong(), Matchers.anyString()))
        .thenThrow(ServiceException.class);

    boolean result = mS3UnderFileSystem.rename(SRC, DST);
    Assert.assertFalse(result);
  }
}
