/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.util.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link BufferUtils} class.
 */
public class BufferUtilsTest {

  /**
   * Tests the {@link BufferUtils#cloneByteBuffer(ByteBuffer)} method.
   */
  @Test
  public void cloneByteBufferTest() {
    final int bufferSize = 10;
    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    ByteBuffer bufClone = null;
    for (byte i = 0; i < bufferSize; i ++) {
      buf.put(i);
    }
    bufClone = BufferUtils.cloneByteBuffer(buf);
    Assert.assertEquals(buf, bufClone);
  }

  /**
   * Tests the {@link BufferUtils#cloneByteBufferList(List)} method.
   */
  @Test
  public void cloneByteBufferListTest() {
    final int bufferSize = 10;
    final int listLength = 10;
    ArrayList<ByteBuffer> bufList = new ArrayList<ByteBuffer>(listLength);
    for (int k = 0; k < listLength; k ++) {
      ByteBuffer buf = ByteBuffer.allocate(bufferSize);
      for (byte i = 0; i < bufferSize; i ++) {
        buf.put((byte) (i + k));
      }
      bufList.add(buf);
    }
    List<ByteBuffer> bufListClone = BufferUtils.cloneByteBufferList(bufList);
    Assert.assertEquals(listLength, bufListClone.size());
    for (int k = 0 ; k < listLength; k ++) {
      Assert.assertEquals(bufList.get(k), bufListClone.get(k));
    }
  }

  /**
   * Tests the {@link BufferUtils#cloneByteBuffer(ByteBuffer)} method after allocating a buffer via
   * {@link ByteBuffer#allocateDirect(int)} method.
   */
  @Test
  public void cloneDirectByteBufferTest() {
    final int bufferSize = 10;
    ByteBuffer bufDirect = ByteBuffer.allocateDirect(bufferSize);
    ByteBuffer bufClone = null;
    for (byte i = 0; i < bufferSize; i ++) {
      bufDirect.put(i);
    }
    bufClone = BufferUtils.cloneByteBuffer(bufDirect);
    Assert.assertEquals(bufDirect, bufClone);
  }

  /**
   * Tests the {@link BufferUtils#cloneByteBufferList(List)} method after allocating a buffer via
   * {@link ByteBuffer#allocateDirect(int)} method.
   */
  @Test
  public void cloneDirectByteBufferListTest() {
    final int bufferSize = 10;
    final int listLength = 10;
    ArrayList<ByteBuffer> bufDirectList = new ArrayList<ByteBuffer>(listLength);
    for (int k = 0; k < listLength; k ++) {
      ByteBuffer bufDirect = ByteBuffer.allocateDirect(bufferSize);
      for (byte i = 0; i < bufferSize; i ++) {
        bufDirect.put((byte) (i + k));
      }
      bufDirectList.add(bufDirect);
    }
    List<ByteBuffer> bufListClone = BufferUtils.cloneByteBufferList(bufDirectList);
    Assert.assertEquals(listLength, bufListClone.size());
    for (int k = 0 ; k < listLength; k ++) {
      Assert.assertEquals(bufDirectList.get(k), bufListClone.get(k));
    }
  }

  /**
   * Tests the {@link BufferUtils#generateNewByteBufferFromThriftRPCResults(ByteBuffer)} method.
   */
  @Test
  public void generateNewByteBufferFromThriftRPCResultsTest() {
    final int bufferSize = 10;
    ByteBuffer mockRPCbuf = ByteBuffer.allocate(bufferSize);
    for (byte i = 0; i < bufferSize; i ++) {
      mockRPCbuf.put(i);
    }
    mockRPCbuf.position(bufferSize / 2);
    ByteBuffer buf = BufferUtils.generateNewByteBufferFromThriftRPCResults(mockRPCbuf);
    Assert.assertEquals(0, buf.position());
    Assert.assertEquals(bufferSize / 2, buf.capacity());
    for (int i = 0; i < bufferSize / 2; i ++) {
      Assert.assertEquals(mockRPCbuf.get(i + bufferSize / 2), buf.get(i));
    }
  }

  /**
   * Tests the {@link BufferUtils#putIntByteBuffer(ByteBuffer, int)} method.
   */
  @Test
  public void putIntByteBufferTest() {
    class TestCase {
      byte mExpected;
      int mInput;

      public TestCase(byte expected, int input) {
        mExpected = expected;
        mInput = input;
      }
    }

    LinkedList<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase((byte) 0x00, 0x00));
    testCases.add(new TestCase((byte) 0x12, 0x12));
    testCases.add(new TestCase((byte) 0x34, 0x1234));
    testCases.add(new TestCase((byte) 0x56, 0x123456));
    testCases.add(new TestCase((byte) 0x78, 0x12345678));

    for (TestCase testCase : testCases) {
      ByteBuffer buf = ByteBuffer.allocate(1);
      BufferUtils.putIntByteBuffer(buf, testCase.mInput);
      Assert.assertEquals(testCase.mExpected, buf.get(0));
    }
  }

  /**
   * Tests the {@link BufferUtils#getIncreasingByteArray(int, int)} method.
   */
  @Test
  public void getIncreasingByteArrayTest() {
    class TestCase {
      byte[] mExpected;
      int mLength;
      int mStart;

      public TestCase(byte[] expected, int length, int start) {
        mExpected = expected;
        mLength = length;
        mStart = start;
      }
    }

    LinkedList<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase(new byte[] {}, 0, 0));
    testCases.add(new TestCase(new byte[] {}, 0, 3));
    testCases.add(new TestCase(new byte[] {0}, 1, 0));
    testCases.add(new TestCase(new byte[] {0, 1, 2}, 3, 0));
    testCases.add(new TestCase(new byte[] {3}, 1, 3));
    testCases.add(new TestCase(new byte[] {3, 4, 5}, 3, 3));

    for (TestCase testCase : testCases) {
      byte[] result = BufferUtils.getIncreasingByteArray(testCase.mStart, testCase.mLength);
      Assert.assertEquals(testCase.mExpected.length, result.length);
      for (int k = 0; k < result.length; k ++) {
        Assert.assertEquals(testCase.mExpected[k], result[k]);
      }
    }
  }

  /**
   * Tests the {@link BufferUtils#equalIncreasingByteArray(int, int, byte[])} method.
   */
  @Test
  public void equalIncreasingByteArrayTest() {
    class TestCase {
      boolean mExpected;
      byte[] mArray;
      int mLength;
      int mStart;

      public TestCase(boolean expected, byte[] array, int length, int start) {
        mExpected = expected;
        mArray = array;
        mLength = length;
        mStart = start;
      }
    }

    LinkedList<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase(false, null, 0, 0));
    testCases.add(new TestCase(true, new byte[] {}, 0, 0));
    testCases.add(new TestCase(false, new byte[] {1}, 0, 0));
    testCases.add(new TestCase(true, new byte[] {}, 0, 3));
    testCases.add(new TestCase(false, new byte[] {1}, 0, 3));
    testCases.add(new TestCase(true, new byte[] {0}, 1, 0));
    testCases.add(new TestCase(false, new byte[] {1}, 1, 0));
    testCases.add(new TestCase(true, new byte[] {0, 1, 2}, 3, 0));
    testCases.add(new TestCase(false, new byte[] {0, 1, 2, (byte) 0xFF}, 3, 0));
    testCases.add(new TestCase(false, new byte[] {1, 2, 3}, 3, 0));
    testCases.add(new TestCase(true, new byte[] {3}, 1, 3));
    testCases.add(new TestCase(false, new byte[] {2}, 1, 3));
    testCases.add(new TestCase(true, new byte[] {3, 4, 5}, 3, 3));
    testCases.add(new TestCase(false, new byte[] {3, 4, 5, (byte) 0xFF}, 3, 3));
    testCases.add(new TestCase(false, new byte[] {2, 3, 4}, 3, 3));

    for (TestCase testCase : testCases) {
      boolean result = BufferUtils.equalIncreasingByteArray(testCase.mStart, testCase.mLength,
          testCase.mArray);
      Assert.assertEquals(testCase.mExpected, result);
    }
  }

  /**
   * Tests the {@link BufferUtils#getIncreasingByteBuffer(int, int)} method.
   */
  @Test
  public void getIncreasingByteBufferTest() {
    class TestCase {
      ByteBuffer mExpected;
      int mLength;
      int mStart;

      public TestCase(ByteBuffer expected, int length, int start) {
        mExpected = expected;
        mLength = length;
        mStart = start;
      }
    }

    LinkedList<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {}), 0, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {}), 0, 3));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {0}), 1, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {0, 1, 2}), 3, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {3}), 1, 3));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {3, 4, 5}), 3, 3));

    for (TestCase testCase : testCases) {
      ByteBuffer result = BufferUtils.getIncreasingByteBuffer(testCase.mStart, testCase.mLength);
      Assert.assertEquals(testCase.mExpected.capacity(), result.capacity());
      for (int k = 0; k < result.capacity(); k ++) {
        Assert.assertEquals(testCase.mExpected.get(k), result.get(k));
      }
    }
  }

  /**
   * Tests the {@link BufferUtils#equalIncreasingByteBuffer(int, int, ByteBuffer)} method.
   */
  @Test
  public void equalIncreasingByteBufferTest() {
    class TestCase {
      boolean mExpected;
      ByteBuffer mBuffer;
      int mLength;
      int mStart;

      public TestCase(boolean expected, ByteBuffer buffer, int length, int start) {
        mExpected = expected;
        mBuffer = buffer;
        mLength = length;
        mStart = start;
      }
    }

    LinkedList<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase(false, null, 0, 0));
    testCases.add(new TestCase(true, ByteBuffer.wrap(new byte[] {}), 0, 0));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {1}), 0, 0));
    testCases.add(new TestCase(true, ByteBuffer.wrap(new byte[] {}), 0, 3));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {1}), 0, 3));
    testCases.add(new TestCase(true, ByteBuffer.wrap(new byte[] {0}), 1, 0));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {1}), 1, 0));
    testCases.add(new TestCase(true, ByteBuffer.wrap(new byte[] {0, 1, 2}), 3, 0));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}), 3, 0));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {1, 2, 3}), 3, 0));
    testCases.add(new TestCase(true, ByteBuffer.wrap(new byte[] {3}), 1, 3));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {2}), 1, 3));
    testCases.add(new TestCase(true, ByteBuffer.wrap(new byte[] {3, 4, 5}), 3, 3));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {3, 4, 5, (byte) 0xFF}), 3, 3));
    testCases.add(new TestCase(false, ByteBuffer.wrap(new byte[] {2, 3, 4}), 3, 3));

    for (TestCase testCase : testCases) {
      boolean result = BufferUtils.equalIncreasingByteBuffer(testCase.mStart, testCase.mLength,
          testCase.mBuffer);
      Assert.assertEquals(testCase.mExpected, result);
    }
  }

  /**
   * Tests the {@link BufferUtils#getIncreasingIntBuffer(int, int)} method.
   */
  @Test
  public void getIncreasingIntBufferTest() {
    class TestCase {
      ByteBuffer mExpected;
      int mLength;
      int mStart;

      public TestCase(ByteBuffer expected, int length, int start) {
        mExpected = expected;
        mLength = length;
        mStart = start;
      }
    }

    LinkedList<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {}), 0, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {}), 0, 3));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {0, 0, 0, 0}), 1, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(
        new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2}), 3, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {0, 0, 0, 3}), 1, 3));
    testCases.add(new TestCase(ByteBuffer.wrap(
        new byte[] {0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5}), 3, 3));

    for (TestCase testCase : testCases) {
      ByteBuffer result = BufferUtils.getIncreasingIntBuffer(testCase.mStart, testCase.mLength);
      Assert.assertEquals(testCase.mExpected.limit(), result.limit());
      for (int k = 0; k < result.limit(); k ++) {
        Assert.assertEquals(testCase.mExpected.get(k), result.get(k));
      }
    }
  }

  /**
   * {@link BufferUtils#cleanDirectBuffer(ByteBuffer)} forces to unmap an unused direct buffer.
   * This test repeated allocates and de-allocates a direct buffer of size 16MB to make sure
   * cleanDirectBuffer is doing its job. The bufferArray is used to store references to the direct
   * buffers so that they won't get garbage collected automatically. It has been tested that if the
   * call to cleanDirectBuffer is removed, this test will fail.
   */
  @Test
  public void cleanDirectBufferTest() {
    final int MAX_ITERATIONS = 1024;
    final int BUFFER_SIZE = 16 * 1024 * 1024;
    // bufferArray keeps reference to each buffer to avoid auto GC
    ByteBuffer[] bufferArray = new ByteBuffer[MAX_ITERATIONS];
    try {
      for (int i = 0; i < MAX_ITERATIONS; i ++) {
        ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        bufferArray[i] = buf;
        BufferUtils.cleanDirectBuffer(buf);
      }
    } catch (OutOfMemoryError ooe) {
      Assert.fail("cleanDirectBuffer is causing memory leak." + ooe.getMessage());
    }
  }

  /**
   * Tests the {@link BufferUtils#sliceByteBuffer(ByteBuffer, int, int)} method
   */
  @Test
  public void sliceByteBufferTest() {
    final int size = 100;
    final ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(size);
    for (int slicePosition : new int[] {0, 1, size / 2, size - 1}) {
      // Slice a ByteBuffer of length 1
      ByteBuffer slicedBuffer = BufferUtils.sliceByteBuffer(buf, slicePosition, 1);
      Assert.assertEquals(0, slicedBuffer.position());
      Assert.assertEquals(1, slicedBuffer.limit());
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(slicePosition, 1, slicedBuffer));

      // Slice a ByteBuffer from the target position to the end
      int slicedBufferLength = size - slicePosition;
      slicedBuffer = BufferUtils.sliceByteBuffer(buf, slicePosition, slicedBufferLength);
      Assert.assertEquals(0, slicedBuffer.position());
      Assert.assertEquals(slicedBufferLength, slicedBuffer.limit());
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(slicePosition, slicedBufferLength,
          slicedBuffer));
    }
  }
}
