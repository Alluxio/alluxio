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

package alluxio.client.file;

import alluxio.client.netty.NettyUnderFileSystemFileReader;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
* Tests {@link alluxio.client.file.UnderFileSystemFileInStream}.
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest(NettyUnderFileSystemFileReader.class)
public class UnderFileSystemFileInStreamTest {
  private static final long FILE_ID = 1L;
  private static final int FILE_SIZE = 1000;

  private byte[] mData;
  private NettyUnderFileSystemFileReader mMockReader;
  private UnderFileSystemFileInStream mTestStream;

  @Before
  public final void before() throws Exception {
    mData = BufferUtils.getIncreasingByteArray(FILE_SIZE);
    mMockReader = PowerMockito.mock(NettyUnderFileSystemFileReader.class);
    InetSocketAddress mockAddr = Mockito.mock(InetSocketAddress.class);
    Mockito.when(mMockReader.read(Mockito.any(InetSocketAddress.class), Mockito.anyLong(),
        Mockito.anyLong(), Mockito.anyLong())).thenAnswer(new Answer<ByteBuffer>() {
          @Override
          public ByteBuffer answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            long offset = (long) args[2];
            long length = (long) args[3];
            if (offset == mData.length) {
              return null;
            }
            int len = (int) Math.min(mData.length - offset, length);
            return ByteBuffer.wrap(mData, (int) offset, len);
          }
        });
    mTestStream = new UnderFileSystemFileInStream(mockAddr, FILE_ID);
    Whitebox.setInternalState(mTestStream, "mReader", mMockReader);
    Whitebox.setInternalState(mTestStream, "mBuffer", ByteBuffer.allocate(FILE_SIZE));
  }

  /**
   * Tests the read() method.
   */
  @Test
  public void readByteTest() throws Exception {
    for (int i = 0; i < FILE_SIZE; i++) {
      Assert.assertEquals((byte) mTestStream.read(), mData[i]);
    }
  }

  /**
   * Tests the internal buffering of the class.
   */
  @Test
  public void readByteBufferedTest() throws Exception {
    for (int i = 0; i < FILE_SIZE; i ++) {
      Assert.assertEquals((byte) mTestStream.read(), mData[i]);
    }
    // One call to get the data since buffer size == file size
    Mockito.verify(mMockReader).read(Mockito.any(InetSocketAddress.class), Mockito.anyLong(),
        Mockito.anyLong(), Mockito.anyLong());
  }

  /**
   * Tests the internal buffering of the class.
   */
  @Test
  public void readBulkBufferedTest() throws Exception {
    byte[] b = new byte[FILE_SIZE / 2];
    Assert.assertEquals(FILE_SIZE / 2, mTestStream.read(b));
    Assert.assertEquals(FILE_SIZE / 2, mTestStream.read(b));
    // One call to get the data since buffer size == file size
    Mockito.verify(mMockReader).read(Mockito.any(InetSocketAddress.class), Mockito.anyLong(),
        Mockito.anyLong(), Mockito.anyLong());
  }

  /**
   * Tests the read(byte[]) method.
   */
  @Test
  public void readByteArrayTest() throws Exception {
    byte[] b = new byte[FILE_SIZE];
    Assert.assertEquals(FILE_SIZE, mTestStream.read(b));
    BufferUtils.equalIncreasingByteArray(FILE_SIZE, b);
  }

  /**
   * Tests the read(byte[], int, int) method.
   */
  @Test
  public void readByteArrayOffsetTest() throws Exception {
    byte[] b = new byte[FILE_SIZE];
    Assert.assertEquals(FILE_SIZE / 2, mTestStream.read(b, FILE_SIZE / 2, FILE_SIZE / 2));
    for (int i = 0; i < FILE_SIZE; i++) {
      Assert.assertEquals((byte) Math.max(0, i - FILE_SIZE / 2), b[i]);
    }
  }

  /**
   * Tests that the stream stops at EOF when reading more than available data and sets the flag
   * appropriately after detecting EOF.
   */
  @Test
  public void readToEOFTest() throws Exception {
    byte[] b = new byte[FILE_SIZE * 100];
    Assert.assertEquals(FILE_SIZE, mTestStream.read(b));
    for (int i = 0; i < FILE_SIZE; i++) {
      Assert.assertEquals((byte) i, b[i]);
    }
    Assert.assertEquals(-1, mTestStream.read());
    boolean eof = Whitebox.getInternalState(mTestStream, "mEOF");
    Assert.assertTrue(eof);
  }
}
