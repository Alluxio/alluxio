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

package alluxio.fuse.file;

import java.nio.ByteBuffer;

/**
 * This interface should be implemented by all fuse file streams.
 */
public interface FuseFileStream extends AutoCloseable {

  /**
   * Reads data from the stream.
   *
   * @param buf the byte buffer to read data to
   * @param size the size to read
   * @param offset the offset of the target stream to begin reading
   * @return the bytes read
   */
  int read(ByteBuffer buf, long size, long offset);

  /**
   * Writes data to the stream.
   *
   * @param buf the byte buffer to read data from and write to the stream
   * @param size the size to write
   * @param offset the offset to write
   */
  void write(ByteBuffer buf, long size, long offset);

  /**
   * @return file status
   */
  FileStatus getFileStatus();

  /**
   * Flushes the stream.
   */
  void flush();

  /**
   * Truncates the file to the given size.
   *
   * @param size the truncate size
   */
  void truncate(long size);

  /**
   * Closes the stream.
   */
  void close();

  /**
   * @return if the stream is closed
   */
  boolean isClosed();
}
