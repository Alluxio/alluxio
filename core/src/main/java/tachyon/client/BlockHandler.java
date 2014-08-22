/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.Constants;

/**
 * Base class for handling block files. Block handlers for different under file systems can be
 * implemented by extending this class.
 */
public abstract class BlockHandler implements Closeable {

  /**
   * Create a block handler according to path scheme
   * 
   * @param path
   *          block file path
   * @return block handler of the block file
   * @throws IOException
   */
  public static BlockHandler get(String path) throws IOException {
    if (path.startsWith(Constants.PATH_SEPARATOR) || path.startsWith("file://")) {
      return new BlockHandlerLocal(path);
    }
    throw new IOException("Unsupported block file path: " + path);
  }

  /**
   * Write data into block file
   * 
   * @param blockOffset
   *          starting position of the block file
   * @param buf
   *          buffer that data is stored in
   * @param offset
   *          offset of the buf
   * @param length
   *          length of the data
   * @return size of data that is written
   * @throws IOException
   */
  public abstract int append(long blockOffset, byte[] buf, int offset, int length)
      throws IOException;

  /**
   * Delete block file
   * 
   * @return true if success, otherwise false
   * @throws IOException
   */
  public abstract boolean delete() throws IOException;

  /**
   * Read data from block file
   * 
   * @param blockOffset
   *          offset from starting of the block file
   * @param length
   *          length of data to read, -1 represents reading the rest of the block file
   * @return byte buffer storing data that is read
   * @throws IOException
   */
  public abstract ByteBuffer read(long blockOffset, int length) throws IOException;
}
