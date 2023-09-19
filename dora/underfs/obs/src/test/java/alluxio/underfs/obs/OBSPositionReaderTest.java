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

package alluxio.underfs.obs;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObsObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;

public class OBSPositionReaderTest {
  /**
   * The OBS Position Reader.
   */
  private OBSPositionReader mOBSPositionReader;
  /**
   * The OBS Client.
   */
  private ObsClient mClient;
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
    mClient = Mockito.mock(ObsClient.class);
    mOBSPositionReader = new OBSPositionReader(mClient, mBucketName, mPath, mFileLength);
  }

  /**
   * Test case for {@link OBSPositionReader#openObjectInputStream(long, int)}.
   */
  @Test
  public void openObjectInputStream() throws Exception {
    ObsObject object = Mockito.mock(ObsObject.class);
    InputStream inputStream = Mockito.mock(InputStream.class);
    Mockito.when(mClient.getObject(ArgumentMatchers.any(
        GetObjectRequest.class))).thenReturn(object);
    Mockito.when(object.getObjectContent()).thenReturn(inputStream);

    // test successful open object input stream
    long position = 0L;
    int bytesToRead = 10;
    Object objectInputStream = mOBSPositionReader.openObjectInputStream(position, bytesToRead);
    Assert.assertTrue(objectInputStream instanceof InputStream);

    // test open object input stream with exception
    Mockito.when(mClient.getObject(ArgumentMatchers.any(GetObjectRequest.class)))
        .thenThrow(ObsException.class);
    try {
      mOBSPositionReader.openObjectInputStream(position, bytesToRead);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
      String errorMessage = String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}
