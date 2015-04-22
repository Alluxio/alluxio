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

import io.netty.channel.FileRegion;

import tachyon.TachyonURI;

/**
 * General interface for handling block I/O. Block handlers for different under file systems
 * implement this interface. It is not thread safe, the caller must guarantee thread safe.
 * This interface is internal and subject to changes.
 */
public interface BlockHandler extends Closeable {

  class Factory {

    /**
     * Create a block handler according to path scheme
     * 
     * @param path the path of the block
     * @return the handler for the block
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static BlockHandler get(String path) throws IOException, IllegalArgumentException {
      if (path.startsWith(TachyonURI.SEPARATOR) || path.startsWith("file://")) {
        return new BlockHandlerLocal(path);
      }
      throw new IllegalArgumentException("Unsupported block file path: " + path);
    }
  }

  /**
   * Delete the block
   * 
   * @return true if success, otherwise false
   * @throws IOException
   */
  boolean delete() throws IOException;

  /**
   * Get file region for some part of block
   * 
   * @param offset the starting position
   * @param length the length of the region
   * @return file region for the specific part of block
   */
  FileRegion getFileRegion(long offset, long length);

  /**
   * Read data from a block
   * 
   * @param position the starting position in the block
   * @param length the size of the data to be read
   * @return the ByteBuffer that contains the data
   * @throws IOException
   */
  ByteBuffer read(long position, int length) throws IOException;

  /**
   * Transfer data from this handler to another handler
   * 
   * @param position the starting position in the block
   * @param length the size of the data to be transferred
   * @param dest the destination handler
   * @param offset the offset in the destination handler
   * @return the size of data that is transferred
   * @throws IOException
   */
  int transferTo(long position, int length, BlockHandler dest, long offset) throws IOException;

  /**
   * Write data to a block
   * 
   * @param position the starting position in the block
   * @param buf the ByteBuffer that contains the data
   * @return the size of data that is written
   * @throws IOException
   */
  int write(long position, ByteBuffer buf) throws IOException;
}
