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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

/**
 * Unit tests for the {@link S3ADirectOutputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(S3ADirectOutputStream.class)
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
    PutObjectRequest request = Mockito.mock(PutObjectRequest.class);

    Mockito.when(request.withMetadata(Mockito.any(ObjectMetadata.class))).thenReturn(request);
    PowerMockito.whenNew(PutObjectRequest.class)
        .withParameterTypes(String.class, String.class, File.class)
        .withArguments(Matchers.eq(BUCKET_NAME), Matchers.eq(KEY), Mockito.any(File.class))
        .thenReturn(request);

    mStream.close();
    Mockito.verify(mManager).upload(request);
  }
}
