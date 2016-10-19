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

package alluxio.underfs.s3;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for {@link S3InputStream}.
 */
public class S3InputStreamTest {

  private static final String BUCKET_NAME = "testBucket";
  private static final String OBJECT_KEY = "testObjectKey";

  private S3InputStream mS3InputStream;
  private S3Service mS3Service;
  private S3Object mS3Object;
  private InputStream mInputStreamSpy;

  @Rule
  public ExpectedException mExceptionRule = ExpectedException.none();

  @Before
  public void setUp() throws ServiceException {
    mInputStreamSpy = Mockito.spy(new ByteArrayInputStream(new byte[] {1, 2, 3}));
    mS3Service = Mockito.mock(S3Service.class);
    mS3Object = Mockito.mock(S3Object.class);
    when(mS3Object.getDataInputStream()).thenReturn(mInputStreamSpy);
    when(mS3Service.getObject(BUCKET_NAME, OBJECT_KEY)).thenReturn(mS3Object);
    mS3InputStream = new S3InputStream(BUCKET_NAME, OBJECT_KEY, mS3Service);
  }

  /**
   * Test of close method, of class S3InputStream.
   */
  @Test
  public void close() throws IOException {
    mS3InputStream.close();
    verify(mInputStreamSpy).close();

    mExceptionRule.expect(IOException.class);
    mExceptionRule.expectMessage(is("Stream closed"));
    mS3InputStream.read();
  }

  /**
   * Test of read method, of class S3InputStream.
   */
  @Test
  public void read() throws IOException {
    assertEquals(1, mS3InputStream.read());
    assertEquals(2, mS3InputStream.read());
    assertEquals(3, mS3InputStream.read());
  }

  /**
   * Test of read method, of class S3InputStream.
   */
  @Test
  public void readWithArgs() throws IOException {
    byte[] bytes = new byte[3];
    int readCount = mS3InputStream.read(bytes, 0, 3);
    assertEquals(3, readCount);
    assertArrayEquals(new byte[] {1, 2, 3}, bytes);
  }

  /**
   * Test of skip method, of class S3InputStream.
   */
  @Test
  public void skip() throws IOException {
    assertEquals(1, mS3InputStream.read());
    mS3InputStream.skip(1);
    assertEquals(3, mS3InputStream.read());
  }
}
