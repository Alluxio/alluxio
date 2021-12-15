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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.anyInt;

import alluxio.retry.CountingRetry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Unit tests for the {@link S3AInputStream}.
 */
public class S3AInputStreamTest {
  private static final String BUCKET_NAME = "testBucket";
  private static final String KEY = "testKey";
  private static final byte[] TESTCONTENT = new byte[]{0x01, 0x02, 0x03, 0x04};
  private AmazonS3 mClient;
  private S3ObjectInputStream[] mIn;
  private S3AInputStream mTestS3InputStream;
  private InputStream[] mTestInputStream;
  private S3Object[] mS3Object;

  @Before
  public void before() throws Exception {
    mTestInputStream = new InputStream[TESTCONTENT.length];
    mS3Object = new S3Object[TESTCONTENT.length];
    mIn = new S3ObjectInputStream[TESTCONTENT.length];
    mClient = Mockito.mock(AmazonS3.class);

    for (int i = 0; i < TESTCONTENT.length; i++) {
      final long pos = i;
      final int finalI = i;
      mS3Object[i] = Mockito.mock(S3Object.class);
      mIn[i] = Mockito.mock(S3ObjectInputStream.class);
      Mockito.when(mClient.getObject(argThat(argument -> {
        if (argument != null) {
          if (argument.getRange() == null) {
            return pos == 0;
          }
          else {
            return argument.getRange()[0] == pos;
          }
        }
        return false;
      }))).thenReturn(mS3Object[i]);
      Mockito.when(mS3Object[i].getObjectContent()).thenReturn(mIn[i]);

      byte[] mockInput = Arrays.copyOfRange(TESTCONTENT, i, TESTCONTENT.length);
      mTestInputStream[i] = new ByteArrayInputStream(mockInput);

      Mockito.when(mIn[i].read()).thenAnswer(invocation -> {
        return mTestInputStream[finalI].read();
      });
      Mockito.when(mIn[i].read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
        byte[] b = invocation.getArgument(0);
        int offset = invocation.getArgument(1);
        int length = invocation.getArgument(2);

        return mTestInputStream[finalI].read(b, offset, length);
      });
    }
    mTestS3InputStream = new S3AInputStream(BUCKET_NAME, KEY, mClient, 0, new CountingRetry(1));
  }

  @Test
  public void readSingleByte() throws Exception {
    Assert.assertEquals((byte) 0x01, mTestS3InputStream.read());
    Assert.assertEquals((byte) 0x02, mTestS3InputStream.read());
    Assert.assertEquals((byte) 0x03, mTestS3InputStream.read());
    Assert.assertEquals((byte) 0x04, mTestS3InputStream.read());
    Assert.assertEquals(-1, mTestS3InputStream.read());
  }

  @Test
  public void skipByte() throws Exception {
    Assert.assertEquals((byte) 0x01, mTestS3InputStream.read());
    mTestS3InputStream.skip(1);
    Assert.assertEquals((byte) 0x03, mTestS3InputStream.read());
  }

  @Test
  public void readByteArray() throws Exception {
    byte[] bytes = new byte[4];
    int readCount = mTestS3InputStream.read(bytes, 0, 4);
    assertEquals(4, readCount);
    assertArrayEquals(TESTCONTENT, bytes);
  }
}
