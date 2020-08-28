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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import com.baidubce.http.BceCloseableHttpResponse;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosObjectInputStream;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.ObjectMetadata;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Unit tests for the {@link BOSInputStream}.
 */
public class BOSInputStreamTest {

  private static final String BUCKET_NAME = "test-alluxio-bos";
  private static final String OBJECT_KEY = "testObjectKey";
  private static AlluxioConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  private BOSInputStream mBosInputStream;
  private BosClient mBosClient;
  private BosObjectInputStream[] mInputStreamSpy;
  private BosObject[] mBosObject;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mExceptionRule = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    mBosClient = mock(BosClient.class);

    byte[] input = new byte[] {1, 2, 3};
    mBosObject = new BosObject[input.length];
    mInputStreamSpy = new BosObjectInputStream[input.length];
    for (int i = 0; i < input.length; ++i) {
      final long pos = (long) i;
      mBosObject[i] = mock(BosObject.class);
      when(mBosClient.getObject(argThat(argument -> {
        if (argument != null) {
          return argument.getRange()[0] == pos;
        }
        return false;
      }))).thenReturn(mBosObject[i]);

      BceCloseableHttpResponse bceCloseableHttpResponse = mock(BceCloseableHttpResponse.class);
      doNothing().when(bceCloseableHttpResponse).close();
      byte[] mockInput = Arrays.copyOfRange(input, i, input.length);
      mInputStreamSpy[i] = spy(new BosObjectInputStream(new ByteArrayInputStream(mockInput),
          bceCloseableHttpResponse));
      when(mBosObject[i].getObjectContent()).thenReturn(mInputStreamSpy[i]);
    }

    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(input.length);
    when(mBosClient.getObjectMetadata(BUCKET_NAME, OBJECT_KEY)).thenReturn(objectMetadata);

    mBosInputStream = new BOSInputStream(BUCKET_NAME, OBJECT_KEY, mBosClient,
        sConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
  }

  @Test
  public void close() throws IOException {
    mBosInputStream.close();

    mExceptionRule.expect(IOException.class);
    mExceptionRule.expectMessage(is("Stream closed"));
    mBosInputStream.read();
  }

  @Test
  public void readInt() throws IOException {
    assertEquals(1, mBosInputStream.read());
    assertEquals(2, mBosInputStream.read());
    assertEquals(3, mBosInputStream.read());
  }

  @Test
  public void readByteArray() throws IOException {
    byte[] bytes = new byte[3];
    int readCount = mBosInputStream.read(bytes, 0, 3);
    assertEquals(3, readCount);
    assertArrayEquals(new byte[] {1, 2, 3}, bytes);
  }

  @Test
  public void skip() throws IOException {
    assertEquals(1, mBosInputStream.read());
    mBosInputStream.skip(1);
    assertEquals(3, mBosInputStream.read());
  }
}
