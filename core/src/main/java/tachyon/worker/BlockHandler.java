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

  long position() throws IOException;

  BlockHandler position(long newPosition) throws IOException;

  long transferTo(long position, long count, WritableByteChannel target) throws IOException;

  FileRegion getFileRegion(long offset, long length);
}
