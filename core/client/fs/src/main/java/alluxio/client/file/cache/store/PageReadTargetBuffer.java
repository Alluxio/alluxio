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

package alluxio.client.file.cache.store;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * TargetBuffer for zero-copy read from page store.
 */
public interface PageReadTargetBuffer {

  /**
   * @return the byte array
   */
  byte[] byteArray();

  /**
   *
   * @return the byte buffer
   */
  ByteBuffer byteBuffer();

  /**
   * @return offset in the buffer
   */
  long offset();

  /**
   * @return the writable channel
   */
  WritableByteChannel byteChannel();

  /**
   * @return the remaining for this buffer
   */
  long remaining();

  /**
   * @param srcArray
   * @param srcOffset
   * @param length
   */
  void writeBytes(byte[] srcArray, int srcOffset, int length) throws IOException;

  /**
   * @param file
   * @param length
   * @return bytes read from the file
   */
  int readFromFile(RandomAccessFile file, int length) throws IOException;
}
