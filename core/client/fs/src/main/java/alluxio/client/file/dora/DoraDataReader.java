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

package alluxio.client.file.dora;

import java.io.Closeable;
import java.nio.channels.WritableByteChannel;

/**
 * DoraDataReader.
 */
public interface DoraDataReader extends Closeable {
  /**
   * Reads data from the source and writes into the output channel.
   *
   * @param offset the offset within the source
   * @param outChannel the output channel
   * @param length number of bytes to read
   * @return actual number of bytes read, -1 when at the end of the source
   * @throws PartialReadException when read was not complete due to an underlying exception
   */
  int read(long offset, WritableByteChannel outChannel, int length)
      throws PartialReadException;

  /**
   * Reads the specified number of bytes into the output channel. This method will
   * try to read as many bytes as requested, possibly by retrying the underlying IO operations if
   * they are retryable.
   *
   * @param offset the offset within the source
   * @param outChannel the output channel
   * @param length number of bytes to read
   * @throws PartialReadException when unretryable IO exceptions occurred
   */
  void readFully(long offset, WritableByteChannel outChannel, int length)
      throws PartialReadException;
}
