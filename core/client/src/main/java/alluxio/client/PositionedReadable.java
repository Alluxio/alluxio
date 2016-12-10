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

import java.io.EOFException;
import java.io.IOException;

/**
 * Stream that permits positional reading.
 *
 * Implementations are required to implement thread-safe operations; this may
 * be supported by concurrent access to the data, or by using a synchronization
 * mechanism to serialize access.
 *
 * Not all implementations meet this requirement. Those that do not cannot
 * be used as a backing store for some applications, such as Apache HBase.
 *
 * Independent of whether or not they are thread safe, some implementations
 * may make the intermediate state of the system, specifically the position
 * obtained in {@code Seekable.getPos()} visible.
 *
 * This interface is directly copied from hadoop repo.
 */
public interface PositionedReadable {
  /**
   * Read up to the specified number of bytes, from a given
   * position within a file, and return the number of bytes read. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @return actual number of bytes read; -1 means "none"
   * @throws IOException IO problems.
   */
  int read(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * Read the specified number of bytes, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @throws IOException IO problems.
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * Read number of bytes equal to the length of the buffer, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @throws IOException IO problems.
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  void readFully(long position, byte[] buffer) throws IOException;
}
