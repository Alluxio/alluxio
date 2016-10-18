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

package alluxio.underfs.oss;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for the {@link OSSInputStream}.
 */
public class OSSInputStreamTest {

  private static final String BUCKET_NAME = "testBucket";
  private static final String OBJECT_KEY = "testObjectKey";

  private OSSInputStream mOssInputStream;
  private OSSClient mOssClient;
  private InputStream mInputStreamSpy;
  private OSSObject mOssObject;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mExceptionRule = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    mOssClient = mock(OSSClient.class);
    mOssObject = mock(OSSObject.class);
    when(mOssClient.getObject(BUCKET_NAME, OBJECT_KEY)).thenReturn(mOssObject);
    mInputStreamSpy = spy(new ByteArrayInputStream(new byte[] {1, 2, 3}));
    when(mOssObject.getObjectContent()).thenReturn(mInputStreamSpy);
    mOssInputStream = new OSSInputStream(BUCKET_NAME, OBJECT_KEY, mOssClient);
  }

  @Test
  public void close() throws IOException {
    mOssInputStream.close();
    verify(mInputStreamSpy).close();

    mExceptionRule.expect(IOException.class);
    mExceptionRule.expectMessage(is("Stream closed"));
    mOssInputStream.read();
  }

  @Test
  public void read() throws IOException {
    assertEquals(1, mOssInputStream.read());
    assertEquals(2, mOssInputStream.read());
    assertEquals(3, mOssInputStream.read());
  }

  @Test
  public void readWithArgs() throws IOException {
    byte[] bytes = new byte[3];
    int readCount = mOssInputStream.read(bytes, 0, 3);
    assertEquals(3, readCount);
    assertArrayEquals(new byte[] {1, 2, 3}, bytes);
  }

  @Test
  public void skip() throws IOException {
    assertEquals(1, mOssInputStream.read());
    mOssInputStream.skip(1);
    assertEquals(3, mOssInputStream.read());
  }

}
