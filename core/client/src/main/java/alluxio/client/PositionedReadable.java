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

package alluxio.client;

import java.io.IOException;

/**
 * Stream that permits positional reading.
 */
public interface PositionedReadable {
  /**
   * Reads up to the specified number of bytes, from a given position within a file, and return the
   * number of bytes read. This does not change the current offset of a file, and is thread-safe.
   *
   * Not all implementations meet the thread safety requirement. According to the documentation of
   * the PositionedReadable interface in Hadoop, those that do not meet the requirement cannot
   * be used as a backing store for some applications, such as Apache HBase.
   *
   * All implementations of this interface in Alluxio must meet the thread safety requirement.
   *
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @return actual number of bytes read; -1 means "EOF";
   */
  int positionedRead(long position, byte[] buffer, int offset, int length) throws IOException;
}
