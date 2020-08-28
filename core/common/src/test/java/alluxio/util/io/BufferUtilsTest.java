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

package alluxio.util.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests the {@link BufferUtils} class.
 */
public final class BufferUtilsTest {

  /**
   * Tests the {@link BufferUtils#cloneByteBuffer(ByteBuffer)} method.
   */
  @Test
  public void cloneByteBuffer() {
    final int bufferSize = 10;
    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    for (byte i = 0; i < bufferSize; i++) {
      buf.put(i);
    }
    ByteBuffer bufClone = BufferUtils.cloneByteBuffer(buf);
    assertEquals(buf, bufClone);
  }

  /**
   * Tests the {@link BufferUtils#cloneByteBufferList(List)} method.
   */
  @Test
  public void cloneByteBufferList() {
    final int bufferSize = 10;
    final int listLength = 10;
    ArrayList<ByteBuffer> bufList = new ArrayList<>(listLength);
    for (int k = 0; k < listLength; k++) {
      ByteBuffer buf = ByteBuffer.allocate(bufferSize);
      for (byte i = 0; i < bufferSize; i++) {
        buf.put((byte) (i + k));
      }
      bufList.add(buf);
    }
    List<ByteBuffer> bufListClone = BufferUtils.cloneByteBufferList(bufList);
    assertEquals(listLength, bufListClone.size());
    for (int k = 0; k < listLength; k++) {
      assertEquals(bufList.get(k), bufListClone.get(k));
    }
  }

  /**
   * Tests the {@link BufferUtils#cloneByteBuffer(ByteBuffer)} method after allocating a buffer via
   * {@link ByteBuffer#allocateDirect(int)} method.
   */
  @Test
  public void cloneDirectByteBuffer() {
    final int bufferSize = 10;
    ByteBuffer bufDirect = ByteBuffer.allocateDirect(bufferSize);
    for (byte i = 0; i < bufferSize; i++) {
      bufDirect.put(i);
    }
    ByteBuffer bufClone = BufferUtils.cloneByteBuffer(bufDirect);
    assertEquals(bufDirect, bufClone);
  }

  /**
   * Tests the {@link BufferUtils#cloneByteBufferList(List)} method after allocating a buffer via
   * {@link ByteBuffer#allocateDirect(int)} method.
   */
  @Test
  public void cloneDirectByteBufferList() {
    final int bufferSize = 10;
    final int listLength = 10;
    ArrayList<ByteBuffer> bufDirectList = new ArrayList<>(listLength);
    for (int k = 0; k < listLength; k++) {
      ByteBuffer bufDirect = ByteBuffer.allocateDirect(bufferSize);
      for (byte i = 0; i < bufferSize; i++) {
        bufDirect.put((byte) (i + k));
      }
      bufDirectList.add(bufDirect);
    }
    List<ByteBuffer> bufListClone = BufferUtils.cloneByteBufferList(bufDirectList);
    assertEquals(listLength, bufListClone.size());
    for (int k = 0; k < listLength; k++) {
      assertEquals(bufDirectList.get(k), bufListClone.get(k));
    }
  }

  /**
   * Tests the {@link BufferUtils#putIntByteBuffer(ByteBuffer, int)} method.
   */
  @Test
  public void putIntByteBuffer() {
    class TestCase {
      byte mExpected;
      int mInput;

      public TestCase(byte expected, int input) {
        mExpected = expected;
        mInput = input;
      }
    }

    ArrayList<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase((byte) 0x00, 0x00));
    testCases.add(new TestCase((byte) 0x12, 0x12));
    testCases.add(new TestCase((byte) 0x34, 0x1234));
    testCases.add(new TestCase((byte) 0x56, 0x123456));
    testCases.add(new TestCase((byte) 0x78, 0x12345678));

    for (TestCase testCase : testCases) {
      ByteBuffer buf = ByteBuffer.allocate(1);
      BufferUtils.putIntByteBuffer(buf, testCase.mInput);
      assertEquals(testCase.mExpected, buf.get(0));
    }
  }

  /**
   * Tests the {@link BufferUtils#getIncreasingByteArray(int, int)} method.
   */
  @Test
  public void getIncreasingByteArray() {
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

    ArrayList<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase(new byte[] {}, 0, 0));
    testCases.add(new TestCase(new byte[] {}, 0, 3));
    testCases.add(new TestCase(new byte[] {0}, 1, 0));
    testCases.add(new TestCase(new byte[] {0, 1, 2}, 3, 0));
    testCases.add(new TestCase(new byte[] {3}, 1, 3));
    testCases.add(new TestCase(new byte[] {3, 4, 5}, 3, 3));

    for (TestCase testCase : testCases) {
      byte[] result = BufferUtils.getIncreasingByteArray(testCase.mStart, testCase.mLength);
      assertEquals(testCase.mExpected.length, result.length);
      for (int k = 0; k < result.length; k++) {
        assertEquals(testCase.mExpected[k], result[k]);
      }
    }
  }

  /**
   * Tests the {@link BufferUtils#equalIncreasingByteArray(int, int, byte[])} method.
   */
  @Test
  public void equalIncreasingByteArray() {
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

    ArrayList<TestCase> testCases = new ArrayList<>();
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
      assertEquals(testCase.mExpected, result);
    }
  }

  /**
   * Tests the {@link BufferUtils#getIncreasingByteBuffer(int, int)} method.
   */
  @Test
  public void getIncreasingByteBuffer() {
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

    ArrayList<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {}), 0, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {}), 0, 3));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {0}), 1, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {0, 1, 2}), 3, 0));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {3}), 1, 3));
    testCases.add(new TestCase(ByteBuffer.wrap(new byte[] {3, 4, 5}), 3, 3));

    for (TestCase testCase : testCases) {
      ByteBuffer result = BufferUtils.getIncreasingByteBuffer(testCase.mStart, testCase.mLength);
      assertEquals(testCase.mExpected.capacity(), result.capacity());
      for (int k = 0; k < result.capacity(); k++) {
        assertEquals(testCase.mExpected.get(k), result.get(k));
      }
    }
  }

  /**
   * Tests the {@link BufferUtils#equalIncreasingByteBuffer(int, int, ByteBuffer)} method.
   */
  @Test
  public void equalIncreasingByteBuffer() {
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

    ArrayList<TestCase> testCases = new ArrayList<>();
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
      assertEquals(testCase.mExpected, result);
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
  public void cleanDirectBuffer() {
    final int MAX_ITERATIONS = 1024;
    final int BUFFER_SIZE = 16 * 1024 * 1024;
    // bufferArray keeps reference to each buffer to avoid auto GC
    ByteBuffer[] bufferArray = new ByteBuffer[MAX_ITERATIONS];
    try {
      for (int i = 0; i < MAX_ITERATIONS; i++) {
        ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        bufferArray[i] = buf;
        BufferUtils.cleanDirectBuffer(buf);
      }
    } catch (OutOfMemoryError ooe) {
      fail("cleanDirectBuffer is causing memory leak." + ooe.getMessage());
    }
  }

  /**
   * Tests the {@link BufferUtils#sliceByteBuffer(ByteBuffer, int, int)} and the
   * {@link BufferUtils#sliceByteBuffer(ByteBuffer, int)} methods.
   */
  @Test
  public void sliceByteBuffer() {
    final int size = 100;
    final ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(size);
    for (int slicePosition : new int[] {0, 1, size / 2, size - 1}) {
      // Slice a ByteBuffer of length 1
      ByteBuffer slicedBuffer = BufferUtils.sliceByteBuffer(buf, slicePosition, 1);
      assertEquals(0, slicedBuffer.position());
      assertEquals(1, slicedBuffer.limit());
      assertTrue(BufferUtils.equalIncreasingByteBuffer(slicePosition, 1, slicedBuffer));

      // Slice a ByteBuffer from the target position to the end
      int slicedBufferLength = size - slicePosition;
      ByteBuffer slicedBuffer1 = BufferUtils.sliceByteBuffer(buf, slicePosition,
          slicedBufferLength);
      ByteBuffer slicedBuffer2 = BufferUtils.sliceByteBuffer(buf, slicePosition);
      assertEquals(0, slicedBuffer1.position());
      assertEquals(0, slicedBuffer2.position());
      assertEquals(slicedBufferLength, slicedBuffer1.limit());
      assertEquals(slicedBufferLength, slicedBuffer2.limit());
      assertTrue(BufferUtils.equalIncreasingByteBuffer(slicePosition, slicedBufferLength,
          slicedBuffer1));
      assertTrue(BufferUtils.equalIncreasingByteBuffer(slicePosition, slicedBufferLength,
          slicedBuffer2));
    }
  }
}
