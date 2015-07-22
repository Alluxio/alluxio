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
import java.util.List;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * Utilities related to buffers, not only ByteBuffer.
 */
public class BufferUtils {
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
      Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
      cleaner.clean();
    }
  }

  /**
   * Clone a bytebuffer.
   * <p>
   * The new bytebuffer will have the same content, but the cloning only applies to array-based
   * bytebuffers
   *
   * @param buf The ByteBuffer to clone
   * @return The new ByteBuffer
   */

  public static ByteBuffer cloneByteBuffer(ByteBuffer buf) {
    ByteBuffer ret = ByteBuffer.allocate(buf.limit() - buf.position());
    if (buf.hasArray()) {
      ret.put(buf.array(), buf.position(), buf.limit() - buf.position());
      ret.flip();
    } else {
      // direct buffer
      ret.put(buf);
      ret.flip();
    }
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
}
