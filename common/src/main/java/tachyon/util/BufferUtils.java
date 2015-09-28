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

package tachyon.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Utility methods for IO buffer related operations.
 */
public final class BufferUtils {
  /**
   * An efficient copy between two channels with a fixed-size buffer.
   *
   * @param src the source channel
   * @param dest the destination channel
   * @throws IOException if the copy fails
   */
  public static void fastCopy(final ReadableByteChannel src, final WritableByteChannel dest)
      throws IOException {
    // TODO: make the buffer size configurable
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

  private BufferUtils() {} // prevent instantiation
}
