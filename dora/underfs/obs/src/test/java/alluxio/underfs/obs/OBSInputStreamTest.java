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

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.retry.CountingRetry;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObsObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Unit tests for the {@link OBSInputStream}.
 */
public class OBSInputStreamTest {

  private static final String BUCKET_NAME = "testBucket";
  private static final String OBJECT_KEY = "testObjectKey";
  private static AlluxioConfiguration sConf = Configuration.global();

  private OBSInputStream mOBSInputStream;
  private ObsClient mObsClient;
  private InputStream[] mInputStreamSpy;
  private ObsObject[] mObjects;

  private static final byte[] INPUT_ARRAY = new byte[] {0, 1, 2};

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mExceptionRule = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    mObsClient = mock(ObsClient.class);

    mObjects = new ObsObject[INPUT_ARRAY.length];
    mInputStreamSpy = new InputStream[INPUT_ARRAY.length];
    for (int i = 0; i < INPUT_ARRAY.length; ++i) {
      final long pos = i;
      mObjects[i] = mock(ObsObject.class);
      when(mObsClient.getObject(argThat(argument -> {
        if (argument instanceof GetObjectRequest) {
          return argument.getRangeStart() == pos;
        }
        return false;
      }))).thenReturn(mObjects[i]);
      byte[] mockInput = Arrays.copyOfRange(INPUT_ARRAY, i, INPUT_ARRAY.length);
      mInputStreamSpy[i] = spy(new ByteArrayInputStream(mockInput));
      when(mObjects[i].getObjectContent()).thenReturn(mInputStreamSpy[i]);
    }
    mOBSInputStream = new OBSInputStream(BUCKET_NAME, OBJECT_KEY, mObsClient, new CountingRetry(1),
        sConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
  }

  @Test
  public void close() throws IOException {
    mOBSInputStream.close();

    mExceptionRule.expect(IOException.class);
    mExceptionRule.expectMessage(is("Stream closed"));
    mOBSInputStream.read();
  }

  @Test
  public void readInt() throws IOException {
    for (int i = 0; i < INPUT_ARRAY.length; ++i) {
      Assert.assertEquals(INPUT_ARRAY[i], mOBSInputStream.read());
    }
  }

  @Test
  public void readByteArray() throws IOException {
    byte[] bytes = new byte[INPUT_ARRAY.length];
    int readCount = mOBSInputStream.read(bytes, 0, INPUT_ARRAY.length);
    Assert.assertEquals(INPUT_ARRAY.length, readCount);
    Assert.assertArrayEquals(INPUT_ARRAY, bytes);
  }

  @Test
  public void skip() throws IOException {
    Assert.assertEquals(0, mOBSInputStream.read());
    mOBSInputStream.skip(1);
    Assert.assertEquals(2, mOBSInputStream.read());
  }

  @Test
  public void testException() {

    for (int i = 0; i < INPUT_ARRAY.length; ++i) {
      when(mObjects[i].getObjectContent()).thenThrow(ObsException.class);
      try {
        mOBSInputStream.read();
      } catch (Exception e) {
        Assert.assertTrue(e instanceof IOException);
      }
    }
  }
}
