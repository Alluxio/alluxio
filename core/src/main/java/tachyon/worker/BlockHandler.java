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

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.FileRegion;

import tachyon.TachyonURI;

/**
 * General interface for handling block I/O. Block handlers for different under file systems can be
 * implemented by extending this class. It is not thread safe, the caller must guarantee thread
 * safe. This class is internal and subject to changes.
 */
public interface BlockHandler extends ByteChannel {

  class Factory {

    /**
     * Create a block handler according to path scheme
     * 
     * @param path the path of the block
     * @return the handler for the block
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static BlockHandler get(String path)
        throws IOException, IllegalArgumentException {
      if (path.startsWith(TachyonURI.SEPARATOR) || path.startsWith("file://")) {
        return new BlockHandlerLocal(path);
      }
      throw new IllegalArgumentException("Unsupported block file path: " + path);
    }
  }
  /**
   * Deletes the block
   * 
   * @return true if success, otherwise false
   * @throws IOException
   */
  boolean delete() throws IOException;

  /**
   * Get the block position of current handler
   * 
   * @return the block position of current handler
   * @throws IOException
   */
  long position() throws IOException;

  /**
   * Set the block position for current handler
   * 
   * @param newPosition the position that will be set
   * @return the handler with new position
   * @throws IOException
   */
  BlockHandler position(long newPosition) throws IOException;

  /**
   * Transfer a part of block to anther writable channel
   * 
   * @param position the starting position
   * @param count the size of data to be transfered
   * @param target the destination channel
   * @return the size that is actually transfered
   * @throws IOException
   */
  long transferTo(long position, long count, WritableByteChannel target) throws IOException;

  /**
   * Get some file region for some part of block
   * 
   * @param offset the starting position
   * @param length the length of the region
   * @return file region for the specific part of block
   */
  FileRegion getFileRegion(long offset, long length);
}
