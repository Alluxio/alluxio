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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.retry.CountingRetry;
import alluxio.util.ConfigurationUtils;

import com.obs.services.ObsClient;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObsObject;
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
  private static AlluxioConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  private OBSInputStream mOBSInputStream;
  private ObsClient mObsClient;
  private InputStream[] mInputStreamSpy;
  private ObsObject[] mObjects;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mExceptionRule = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    mObsClient = mock(ObsClient.class);

    byte[] input = new byte[]{1, 2, 3};
    mObjects = new ObsObject[input.length];
    mInputStreamSpy = new InputStream[input.length];
    for (int i = 0; i < input.length; ++i) {
      final long pos = i;
      mObjects[i] = mock(ObsObject.class);
      when(mObsClient.getObject(argThat(argument -> {
        if (argument instanceof GetObjectRequest) {
          return ((GetObjectRequest) argument).getRangeStart() == pos;
        }
        return false;
      }))).thenReturn(mObjects[i]);
      byte[] mockInput = Arrays.copyOfRange(input, i, input.length);
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
    assertEquals(1, mOBSInputStream.read());
    assertEquals(2, mOBSInputStream.read());
    assertEquals(3, mOBSInputStream.read());
  }

  @Test
  public void readByteArray() throws IOException {
    byte[] bytes = new byte[3];
    int readCount = mOBSInputStream.read(bytes, 0, 3);
    assertEquals(3, readCount);
    assertArrayEquals(new byte[]{1, 2, 3}, bytes);
  }

  @Test
  public void skip() throws IOException {
    assertEquals(1, mOBSInputStream.read());
    mOBSInputStream.skip(1);
    assertEquals(3, mOBSInputStream.read());
  }
}
