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

import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class GCSPositionReaderTest {
  /**
   * The GCS Position Reader.
   */
  private GCSPositionReader mPositionReader;
  /**
   * The COS Client.
   */
  private GoogleStorageService mClient;
  /**
   * The Bucket Name.
   */
  private final String mBucketName = "bucket";
  /**
   * The Path (or the Key).
   */
  private final String mPath = "path";
  /**
   * The File Length.
   */
  private final long mFileLength = 100L;

  @Before
  public void before() throws Exception {
    mClient = Mockito.mock(GoogleStorageService.class);
    mPositionReader = new GCSPositionReader(mClient, mBucketName, mPath, mFileLength);
  }

  /**
   * Test case for {@link GCSPositionReader#openObjectInputStream(long, int)}.
   */
  @Test
  public void openObjectInputStream() throws Exception {
    GSObject object = Mockito.mock(GSObject.class);
    BufferedInputStream objectInputStream = Mockito.mock(BufferedInputStream.class);
    Mockito.when(mClient.getObject(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
        Mockito.any(), Mockito.any())).thenReturn(object);
    Mockito.when(object.getDataInputStream()).thenReturn(objectInputStream);

    // test successful open object input stream
    long position = 0L;
    int bytesToRead = 10;
    InputStream inputStream = mPositionReader.openObjectInputStream(position, bytesToRead);
    Assert.assertTrue(inputStream instanceof BufferedInputStream);

    // test open object input stream with exception
    Mockito.when(mClient.getObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenThrow(ServiceException.class);
    try {
      mPositionReader.openObjectInputStream(position, bytesToRead);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
      String errorMessage = String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}
