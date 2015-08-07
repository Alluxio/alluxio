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

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
/**
 * Utilities related to buffers, not only ByteBuffer.
 */
public class BufferUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static Method sCleanMethod;
  private static Method sCleanerMethod;

  /**
   * Force to unmap direct buffer if the buffer is no longer used. It is unsafe operation and
   * currently a walk-around to avoid huge memory occupation caused by memory map.
   *
   * @param buffer the byte buffer to be unmapped
   */
  public static void cleanDirectBuffer(ByteBuffer buffer) {
    if (buffer == null) {
      return;
    }
    if (buffer.isDirect()) {
      try {
        if (sCleanerMethod == null) {
          sCleanerMethod = buffer.getClass().getMethod("cleaner");
          sCleanerMethod.setAccessible(true);
        }
        final Object cleaner = sCleanerMethod.invoke(buffer);
        if (cleaner != null) {
          if (sCleanMethod == null) {
            sCleanMethod = cleaner.getClass().getMethod("clean");
          }
          sCleanMethod.invoke(cleaner);
        }
      } catch (Exception e) {
        LOG.warn("Fail to unmap direct buffer due to ", e);
        buffer = null;
      }
    }
  }

  /**
   * Clone a bytebuffer.
   * <p>
   * The new bytebuffer will have the same content, but the type of the bytebuffer may not be
   * the same.
   *
   * @param buf The ByteBuffer to clone
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

  public static List<ByteBuffer> cloneByteBufferList(List<ByteBuffer> source) {
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>(source.size());
    for (ByteBuffer b : source) {
      ret.add(cloneByteBuffer(b));
    }
    return ret;
  }

  public static ByteBuffer generateNewByteBufferFromThriftRPCResults(ByteBuffer data) {
    // TODO this is a trick to fix the issue in thrift. Change the code to use
    // metadata directly when thrift fixes the issue.
    ByteBuffer correctData = ByteBuffer.allocate(data.limit() - data.position());
    correctData.put(data);
    correctData.flip();
    return correctData;
  }

  public static void putIntByteBuffer(ByteBuffer buf, int b) {
    buf.put((byte) (b & 0xFF));
  }

  public static byte[] getIncreasingByteArray(int len) {
    return getIncreasingByteArray(0, len);
  }

  public static byte[] getIncreasingByteArray(int start, int len) {
    byte[] ret = new byte[len];
    for (int k = 0; k < len; k ++) {
      ret[k] = (byte) (k + start);
    }
    return ret;
  }

  public static boolean equalIncreasingByteArray(int len, byte[] arr) {
    return equalIncreasingByteArray(0, len, arr);
  }

  public static boolean equalIncreasingByteArray(int start, int len, byte[] arr) {
    if (arr == null || arr.length < len) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      if (arr[k] != (byte) (start + k)) {
        return false;
      }
    }
    return true;
  }

  public static ByteBuffer getIncreasingByteBuffer(int len) {
    return getIncreasingByteBuffer(0, len);
  }

  public static ByteBuffer getIncreasingByteBuffer(int start, int len) {
    return ByteBuffer.wrap(getIncreasingByteArray(start, len));
  }

  public static boolean equalIncreasingByteBuffer(int start, int len, ByteBuffer buf) {
    if (buf == null) {
      return false;
    }
    buf.rewind();
    if (buf.remaining() != len) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      if (buf.get() != (byte) (start + k)) {
        return false;
      }
    }
    return true;
  }

  public static ByteBuffer getIncreasingIntBuffer(int len) {
    ByteBuffer ret = ByteBuffer.allocate(len * 4);
    for (int k = 0; k < len; k ++) {
      ret.putInt(k);
    }
    ret.flip();
    return ret;
  }

  /**
   * * Writes buffer to the given file path
   *
   * @param path file path to write the data
   * @param buffer raw data
   * @throws java.io.IOException
   */
  public static void writeBufferToFile(String path, byte[] buffer) throws IOException {
    FileOutputStream os = new FileOutputStream(path);
    os.write(buffer);
    os.close();
  }
}
