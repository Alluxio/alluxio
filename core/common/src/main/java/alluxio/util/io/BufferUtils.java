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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities related to buffers, not only {@link ByteBuffer}.
 */
@ThreadSafe
public final class BufferUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BufferUtils.class);
  private static final Object LOCK = new Object();

  private static Method sCleanerCleanMethod;
  private static Method sByteBufferCleanerMethod;

  /**
   * Converts a byte to an integer.
   *
   * @param b the byte to convert
   * @return the integer representation of the byte
   */
  public static int byteToInt(byte b) {
    return b & 0xFF;
  }

  /**
   * Forces to unmap a direct buffer if this buffer is no longer used. After calling this method,
   * this direct buffer should be discarded. This is unsafe operation and currently a work-around to
   * avoid huge memory occupation caused by memory map.
   *
   * <p>
   * NOTE: DirectByteBuffers are not guaranteed to be garbage-collected immediately after their
   * references are released and may lead to OutOfMemoryError. This function helps by calling the
   * Cleaner method of a DirectByteBuffer explicitly. See <a
   * href="http://stackoverflow.com/questions/1854398/how-to-garbage-collect-a-direct-buffer-java"
   * >more discussion</a>.
   *
   * @param buffer the byte buffer to be unmapped, this must be a direct buffer
   */
  public static void cleanDirectBuffer(ByteBuffer buffer) {
    Preconditions.checkNotNull(buffer, "buffer");
    Preconditions.checkArgument(buffer.isDirect(), "buffer isn't a DirectByteBuffer");
    if (sByteBufferCleanerMethod == null) {
      synchronized (LOCK) {
        try {
          if (sByteBufferCleanerMethod == null) {
            sByteBufferCleanerMethod = buffer.getClass().getMethod("cleaner");
            sByteBufferCleanerMethod.setAccessible(true);
          }
        } catch (Exception e) {
          // Force to drop reference to the buffer to clean
          buffer = null;
          return;
        }
      }
    }
    try {
      final Object cleaner = sByteBufferCleanerMethod.invoke(buffer);
      if (cleaner == null) {
        if (buffer.capacity() > 0) {
          LOG.warn("Failed to get cleaner for ByteBuffer: {}", buffer.getClass().getName());
        }
        // The cleaner could be null when the buffer is initialized as zero capacity.
        return;
      }
      if (sCleanerCleanMethod == null) {
        synchronized (LOCK) {
          if (sCleanerCleanMethod == null) {
            sCleanerCleanMethod = cleaner.getClass().getMethod("clean");
          }
        }
      }
      sCleanerCleanMethod.invoke(cleaner);
    } catch (Exception e) {
      LOG.warn("Failed to unmap direct ByteBuffer: {}, error message: {}",
                buffer.getClass().getName(), e.getMessage());
    } finally {
      // Force to drop reference to the buffer to clean
      buffer = null;
    }
  }

  /**
   * Clones a {@link ByteBuffer}.
   * <p>
   * The new bytebuffer will have the same content, but the type of the bytebuffer may not be the
   * same.
   *
   * @param buf The ByteBuffer to copy
   * @return The new ByteBuffer
   */
  public static ByteBuffer cloneByteBuffer(ByteBuffer buf) {
    ByteBuffer ret = ByteBuffer.allocate(buf.limit() - buf.position());
    if (buf.hasArray()) {
      ret.put(buf.array(), buf.position(), buf.limit() - buf.position());
    } else {
      // direct buffer
      ret.put(buf);
    }
    ret.flip();
    return ret;
  }

  /**
   * Clones a list of {@link ByteBuffer}s.
   *
   * @param source the list of ByteBuffers to copy
   * @return the new list of ByteBuffers
   */
  public static List<ByteBuffer> cloneByteBufferList(List<ByteBuffer> source) {
    List<ByteBuffer> ret = new ArrayList<>(source.size());
    for (ByteBuffer b : source) {
      ret.add(cloneByteBuffer(b));
    }
    return ret;
  }

  /**
   * Puts a byte (the first byte of an integer) into a {@link ByteBuffer}.
   *
   * @param buf ByteBuffer to use
   * @param b byte to put into the ByteBuffer
   */
  public static void putIntByteBuffer(ByteBuffer buf, int b) {
    buf.put((byte) (b & 0xFF));
  }

  /**
   * Gets an increasing sequence of bytes starting at zero.
   *
   * @param len the target length of the sequence
   * @return an increasing sequence of bytes
   */
  public static byte[] getIncreasingByteArray(int len) {
    return getIncreasingByteArray(0, len);
  }

  /**
   * Gets an increasing sequence of bytes starting with the given value.
   *
   * @param start the starting value to use
   * @param len the target length of the sequence
   * @return an increasing sequence of bytes
   */
  public static byte[] getIncreasingByteArray(int start, int len) {
    byte[] ret = new byte[len];
    for (int k = 0; k < len; k++) {
      ret[k] = (byte) (k + start);
    }
    return ret;
  }

  /**
   * Checks if the given byte array starts with a constant sequence of bytes of the given value
   * and length.
   *
   * @param value the value to check for
   * @param len the target length of the sequence
   * @param arr the byte array to check
   * @return true if the byte array has a prefix of length {@code len} that is a constant
   *         sequence of bytes of the given value
   */
  public static boolean equalConstantByteArray(byte value, int len, byte[] arr) {
    if (arr == null || arr.length != len) {
      return false;
    }
    for (int k = 0; k < len; k++) {
      if (arr[k] != value) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the given byte array starts with an increasing sequence of bytes of the given
   * length, starting from 0.
   *
   * @param len the target length of the sequence
   * @param arr the byte array to check
   * @return true if the byte array has a prefix of length {@code len} that is an increasing
   *         sequence of bytes starting at zero
   */
  public static boolean equalIncreasingByteArray(int len, byte[] arr) {
    return equalIncreasingByteArray(0, len, arr);
  }

  /**
   * Checks if the given byte array starts with an increasing sequence of bytes of the given
   * length, starting from the given value. The array length must be equal to the length checked.
   *
   * @param start the starting value to use
   * @param len the target length of the sequence
   * @param arr the byte array to check
   * @return true if the byte array has a prefix of length {@code len} that is an increasing
   *         sequence of bytes starting at {@code start}
   */
  public static boolean equalIncreasingByteArray(int start, int len, byte[] arr) {
    if (arr == null || arr.length != len) {
      return false;
    }
    for (int k = 0; k < len; k++) {
      if (arr[k] != (byte) (start + k)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets a {@link ByteBuffer} containing an increasing sequence of bytes starting at zero.
   *
   * @param len the target length of the sequence
   * @return ByteBuffer containing an increasing sequence of bytes
   */
  public static ByteBuffer getIncreasingByteBuffer(int len) {
    return getIncreasingByteBuffer(0, len);
  }

  /**
   * Gets a {@link ByteBuffer} containing an increasing sequence of bytes starting at the given
   * value.
   *
   * @param len the target length of the sequence
   * @param start the starting value to use
   * @return ByteBuffer containing an increasing sequence of bytes
   */
  public static ByteBuffer getIncreasingByteBuffer(int start, int len) {
    return ByteBuffer.wrap(getIncreasingByteArray(start, len));
  }

  /**
   * Checks if the given {@link ByteBuffer} starts with an increasing sequence of bytes starting at
   * the given value of length equal to or greater than the given length.
   *
   * @param start the starting value to use
   * @param len the target length of the sequence
   * @param buf the ByteBuffer to check
   * @return true if the ByteBuffer has a prefix of length {@code len} that is an increasing
   *         sequence of bytes starting at {@code start}
   */
  public static boolean equalIncreasingByteBuffer(int start, int len, ByteBuffer buf) {
    if (buf == null) {
      return false;
    }
    buf.rewind();
    if (buf.remaining() != len) {
      return false;
    }
    for (int k = 0; k < len; k++) {
      if (buf.get() != (byte) (start + k)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Writes buffer to the given file path.
   *
   * @param path file path to write the data
   * @param buffer raw data
   */
  public static void writeBufferToFile(String path, byte[] buffer) throws IOException {
    try (FileOutputStream os = new FileOutputStream(path)) {
      os.write(buffer);
    }
  }

  /**
   * An efficient copy between two channels with a fixed-size buffer.
   *
   * @param src the source channel
   * @param dest the destination channel
   */
  public static void fastCopy(final ReadableByteChannel src, final WritableByteChannel dest)
      throws IOException {
    // TODO(yupeng): make the buffer size configurable
    final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);

    while (src.read(buffer) != -1) {
      buffer.flip();
      dest.write(buffer);
      buffer.compact();
    }

    buffer.flip();

    while (buffer.hasRemaining()) {
      dest.write(buffer);
    }
  }

  /**
   * Gets the unsigned byte value of an integer. Useful for comparing to the result of reading
   * from an input stream.
   * @param i the integer to convert
   * @return the integer value after casting as an unsigned byte
   */
  public static int intAsUnsignedByteValue(int i) {
    return ((byte) i) & 0xFF;
  }

  /**
   * Creates a byte array from the given ByteBuffer, the position property of input
   * {@link ByteBuffer} remains unchanged.
   *
   * @param buf source ByteBuffer
   * @return a newly created byte array
   */
  public static byte[] newByteArrayFromByteBuffer(ByteBuffer buf) {
    final int length = buf.remaining();
    byte[] bytes = new byte[length];
    // transfer bytes from this buffer into the given destination array
    buf.duplicate().get(bytes, 0, length);
    return bytes;
  }

  /**
   * Creates a new ByteBuffer sliced from a given ByteBuffer. The new ByteBuffer shares the
   * content of the existing one, but with independent position/mark/limit. After slicing, the
   * new ByteBuffer has position 0, and the input ByteBuffer is unmodified.
   *
   * @param buffer source ByteBuffer to slice
   * @param position position in the source ByteBuffer to slice
   * @param length length of the sliced ByteBuffer
   * @return the sliced ByteBuffer
   */
  public static ByteBuffer sliceByteBuffer(ByteBuffer buffer, int position, int length) {
    ByteBuffer slicedBuffer = ((ByteBuffer) buffer.duplicate().position(position)).slice();
    slicedBuffer.limit(length);
    return slicedBuffer;
  }

  /**
   * Convenience method for {@link #sliceByteBuffer(ByteBuffer, int, int)} where the last parameter
   * is the number of remaining bytes in the new buffer.
   *
   * @param buffer source {@link ByteBuffer} to slice
   * @param position position in the source {@link ByteBuffer} to slice
   * @return the sliced {@link ByteBuffer}
   */
  public static ByteBuffer sliceByteBuffer(ByteBuffer buffer, int position) {
    // The following is an optimization comparing to directly calling
    // sliceByteBuffer(ByteBuffer, int, int) needs to compute the length of the sliced buffer and
    // set the limit, but those operations should have been taken care of by the slice() method.
    return ((ByteBuffer) buffer.duplicate().position(position)).slice();
  }

  private BufferUtils() {} // prevent instantiation
}
