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

package alluxio.underfs.b2;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentHandlers.B2ContentMemoryWriter;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.google.protobuf.ServiceException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Unit tests for {@link B2InputStream}.
 */
public class B2InputStreamTest {

  private static final String BUCKET_NAME = "testBucket";
  private static final String OBJECT_KEY = "testObjectKey";
  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();
  @Mock
  private B2ContentMemoryWriter mB2ContentMemoryWriter;
  private B2InputStream mB2InputStream;
  private B2StorageClient mB2StorageClient;
  private byte[] mBytes;

  @Before
  public void setUp() throws ServiceException {
    MockitoAnnotations.initMocks(this);

    mBytes = new byte[] {1, 2, 3};
    mB2StorageClient = mock(B2StorageClient.class);
    mB2ContentMemoryWriter = mock(B2ContentMemoryWriter.class);

    when(mB2ContentMemoryWriter.getBytes()).thenReturn(mBytes);

    mB2InputStream = new B2InputStream(BUCKET_NAME, OBJECT_KEY, mB2StorageClient, 0L, 100L) {
      @Override
      protected B2ContentMemoryWriter getSink() {
        return mB2ContentMemoryWriter;
      }
    };
  }

  /**
   * Test of close method, of class B2InputStream.
   */
  @Test
  public void close() throws IOException {
    mB2InputStream.close();

    mExpectedException.expect(IOException.class);
    mExpectedException.expectMessage(is("Stream closed"));
    mB2InputStream.read();
  }

  /**
   * Test of read method, of class B2InputStream.
   */
  @Test
  public void read() throws IOException, B2Exception, ServiceException {

    assertEquals(1, mB2InputStream.read());
    assertEquals(2, mB2InputStream.read());
    assertEquals(3, mB2InputStream.read());
  }

  /**
   * Test of read method, of class B2InputStream.
   */
  @Test
  public void readWithArgs() throws IOException {
    byte[] bytes = new byte[3];
    int readCount = mB2InputStream.read(bytes, 0, 3);
    assertEquals(3, readCount);
    assertArrayEquals(new byte[] {1, 2, 3}, bytes);
  }

  /**
   * Test of skip method, of class B2InputStream.
   */
  @Test
  public void skip() throws IOException {
    assertEquals(1, mB2InputStream.read());
    mB2InputStream.skip(1);
    assertEquals(3, mB2InputStream.read());
  }
}
