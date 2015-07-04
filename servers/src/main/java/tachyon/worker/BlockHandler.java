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

package tachyon.worker;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import tachyon.TachyonURI;

/**
 * Base class for handling block I/O. Block handlers for different under file systems can be
 * implemented by extending this class. It is not thread safe, the caller must guarantee thread
 * safe. This class is internal and subject to changes.
 */
public abstract class BlockHandler implements Closeable {

  /**
   * Create a block handler according to path scheme
   * 
   * @param path the block path
   * @return the handler of the block
   * @throws IOException
   * @throws IllegalArgumentException
   */
  public static BlockHandler get(String path) throws IOException, IllegalArgumentException {
    if (path.startsWith(TachyonURI.SEPARATOR) || path.startsWith("file://")) {
      return new BlockHandlerLocal(path);
    }
    throw new IllegalArgumentException("Unsupported block file path: " + path);
  }

  /**
   * Append data to the block from a byte array
   * 
   * @param blockOffset starting position of the block file
   * @param buf the data buffer
   * @param offset the offset of the buffer
   * @param length the length of the data
   * @return the size of data that was written
   * @throws IOException
   */
  public int append(long blockOffset, byte[] buf, int offset, int length) throws IOException {
    return append(blockOffset, ByteBuffer.wrap(buf, offset, length));
  }

  /**
   * Appends data to the block from a ByteBuffer
   * 
   * @param blockOffset starting position of the block file
   * @param srcBuf ByteBuffer that data is stored in
   * @return the size of data that was written
   * @throws IOException
   */
  public abstract int append(long blockOffset, ByteBuffer srcBuf) throws IOException;

  /**
   * Deletes the block
   * 
   * @return true if success, otherwise false
   * @throws IOException
   */
  public abstract boolean delete() throws IOException;

  /**
   * Gets channel used to access block
   * 
   * @return the channel bounded with the block file
   */
  public abstract ByteChannel getChannel();

  /**
   * Gets the length of the block
   * 
   * @return the length of the block
   * @throws IOException
   */
  public abstract long getLength() throws IOException;

  /**
   * Reads data from block
   * 
   * @param offset the offset from starting of the block file
   * @param length the length of data to read, -1 represents reading the rest of the block
   * @return ByteBuffer the data that was read
   * @throws IOException
   */
  public abstract ByteBuffer read(long offset, int length) throws IOException;
}
