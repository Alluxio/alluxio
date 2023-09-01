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

package alluxio.underfs.cos;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.retry.CountingRetry;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Unit tests for the {@link COSInputStream}.
 */
public class COSInputStreamTest {

  private static final String BUCKET_NAME = "testBucket";
  private static final String OBJECT_KEY = "testObjectKey";
  private static AlluxioConfiguration sConf = Configuration.global();

  private COSInputStream mCosInputStream;
  private COSClient mCosClient;
  private COSObjectInputStream[] mCOSObjectInputStreamSpy;
  private COSObject[] mCosObject;

  private static final byte[] INPUT_ARRAY = new byte[] {0, 1, 2};

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mExceptionRule = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    mCosClient = mock(COSClient.class);

    mCosObject = new COSObject[INPUT_ARRAY.length];
    mCOSObjectInputStreamSpy = new COSObjectInputStream[INPUT_ARRAY.length];
    for (int i = 0; i < INPUT_ARRAY.length; ++i) {
      final long pos = (long) i;
      mCosObject[i] = mock(COSObject.class);
      when(mCosClient.getObject(argThat(argument -> {
        if (argument != null) {
          return argument.getRange()[0] == pos;
        }
        return false;
      })))
          .thenReturn(mCosObject[i]);
      byte[] mockInput = Arrays.copyOfRange(INPUT_ARRAY, i, INPUT_ARRAY.length);
      mCOSObjectInputStreamSpy[i] = spy(new COSObjectInputStream(
          new ByteArrayInputStream(mockInput), null));
      when(mCosObject[i].getObjectContent()).thenReturn(mCOSObjectInputStreamSpy[i]);
    }
    mCosInputStream = new COSInputStream(BUCKET_NAME, OBJECT_KEY, mCosClient, new CountingRetry(1),
        sConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
  }

  @Test
  public void close() throws IOException {
    mCosInputStream.close();
    mExceptionRule.expect(IOException.class);
    mExceptionRule.expectMessage(is("Stream closed"));
    mCosInputStream.read();
  }

  @Test
  public void readInt() throws IOException {
    for (int i = 0; i < INPUT_ARRAY.length; ++i) {
      Assert.assertEquals(INPUT_ARRAY[i], mCosInputStream.read());
    }
  }

  @Test
  public void readByteArray() throws IOException {
    byte[] bytes = new byte[INPUT_ARRAY.length];
    int readCount = mCosInputStream.read(bytes, 0, INPUT_ARRAY.length);
    Assert.assertEquals(INPUT_ARRAY.length, readCount);
    Assert.assertArrayEquals(INPUT_ARRAY, bytes);
  }

  @Test
  public void skip() throws IOException {
    Assert.assertEquals(0, mCosInputStream.read());
    mCosInputStream.skip(1);
    Assert.assertEquals(2, mCosInputStream.read());
  }

  @Test
  public void testException() {

    for (int i = 0; i < INPUT_ARRAY.length; ++i) {
      when(mCosObject[i].getObjectContent()).thenThrow(new CosServiceException(""));
      try {
        mCosInputStream.read();
      } catch (Exception e) {
        Assert.assertTrue(e instanceof IOException);
      }
    }
  }
}
