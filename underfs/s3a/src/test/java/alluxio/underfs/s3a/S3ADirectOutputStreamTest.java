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

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for the {@link S3ADirectOutputStream}.
 */
public class S3ADirectOutputStreamTest {
  private static final String BUCKET_NAME = "testBucket";
  private static final String KEY = "testKey";

  private TransferManager mManager;
  private S3ADirectOutputStream mStream;

  /**
   * Sets the properties and configuration before each test runs.
   */
  @Before
  public void before() throws Exception {
    mManager = Mockito.mock(TransferManager.class);
    Upload result = Mockito.mock(Upload.class);

    Mockito.when(mManager.upload(Mockito.any(PutObjectRequest.class))).thenReturn(result);
    mStream = new S3ADirectOutputStream(BUCKET_NAME, KEY, mManager);
  }

  /**
   * Tests to ensure the permanent file path is used when upload is called.
   */
  @Test
  public void close() throws Exception {
    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    mStream.close();
    Mockito.verify(mManager).upload(captor.capture());
    Assert.assertEquals(KEY, captor.getValue().getKey());
  }
}
