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

package alluxio.underfs.s3a;

import alluxio.AlluxioURI;
import alluxio.underfs.options.DeleteOptions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for the {@link S3AUnderFileSystem}.
 */
public class S3AUnderFileSystemTest {
  private S3AUnderFileSystem mS3UnderFileSystem;
  private AmazonS3Client mClient;
  private TransferManager mManager;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final short BUCKET_MODE = 0;
  private static final String ACCOUNT_OWNER = "account owner";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, AmazonClientException {
    mClient = Mockito.mock(AmazonS3Client.class);
    mManager = Mockito.mock(TransferManager.class);
    mS3UnderFileSystem = new S3AUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        BUCKET_MODE, ACCOUNT_OWNER, mManager, null);
  }

  /**
   * Test case for {@link S3AUnderFileSystem#delete(String, boolean)}.
   */
  @Test
  public void deleteNonRecursiveOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    boolean result = mS3UnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link S3AUnderFileSystem#delete(String, boolean)}.
   */
  @Test
  public void deleteRecursiveOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    boolean result = mS3UnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link S3AUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    boolean result = mS3UnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }
}
