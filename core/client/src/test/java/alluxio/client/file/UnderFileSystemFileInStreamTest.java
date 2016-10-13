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

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.netty.NettyUnderFileSystemFileReader;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
       PropertyKey.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES, Long.toString(FILE_SIZE)));

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
    mTestStream = new UnderFileSystemFileInStream(mockAddr, FILE_ID, mMockReader);
  }

  /**
   * Tests the read() method.
   */
  @Test
  public void readByte() throws Exception {
    for (int i = 0; i < FILE_SIZE; i++) {
      Assert.assertEquals((byte) mTestStream.read(), mData[i]);
    }
  }

  /**
   * Tests the internal buffering of the class.
   */
  @Test
  public void readByteBuffered() throws Exception {
    for (int i = 0; i < FILE_SIZE; i++) {
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
  public void readBulkBuffered() throws Exception {
    int readLen = FILE_SIZE / 2;
    byte[] b = new byte[readLen];
    Assert.assertEquals(readLen, mTestStream.read(b));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(0, readLen, b));
    Assert.assertEquals(readLen, mTestStream.read(b));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(readLen, readLen, b));
    // One call to get the data since buffer size == file size
    Mockito.verify(mMockReader).read(Mockito.any(InetSocketAddress.class), Mockito.anyLong(),
        Mockito.anyLong(), Mockito.anyLong());
  }

  /**
   * Tests the read(byte[]) method.
   */
  @Test
  public void readByteArray() throws Exception {
    byte[] b = new byte[FILE_SIZE];
    Assert.assertEquals(FILE_SIZE, mTestStream.read(b));
    BufferUtils.equalIncreasingByteArray(FILE_SIZE, b);
  }

  /**
   * Tests the read(byte[], int, int) method.
   */
  @Test
  public void readByteArrayOffset() throws Exception {
    byte[] b = new byte[FILE_SIZE];
    Assert.assertEquals(FILE_SIZE / 2, mTestStream.read(b, FILE_SIZE / 2, FILE_SIZE / 2));
    for (int i = 0; i < FILE_SIZE; i++) {
      Assert.assertEquals((byte) Math.max(0, i - FILE_SIZE / 2), b[i]);
    }
  }

  /**
   * Tests that the stream stops at EOF when reading more than available data.
   */
  @Test
  public void readToEOF() throws Exception {
    byte[] b = new byte[FILE_SIZE * 100];
    Assert.assertEquals(FILE_SIZE, mTestStream.read(b));
    for (int i = 0; i < FILE_SIZE; i++) {
      Assert.assertEquals((byte) i, b[i]);
    }
    Assert.assertEquals(-1, mTestStream.read());
  }
}
