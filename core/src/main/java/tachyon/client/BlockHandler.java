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

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import tachyon.Constants;

/**
 * It is the base class for handling block files. Block handlers for different under
 * file systems can be implemented by extending this class. It also creates a specific
 * BlockHandler for certain block file by checking the block file's path.
 */
public abstract class BlockHandler implements Closeable {
  /**
   * Create a block handler according to path scheme
   * 
   * @param path
   *          block file path
   * @return block handler of the block file
   * @throws IOException
   * @throws IllegalArgumentException
   */
  public static BlockHandler get(String path) throws IOException, IllegalArgumentException {
    if (path.startsWith(Constants.PATH_SEPARATOR) || path.startsWith("file://")) {
      return new BlockHandlerLocal(path);
    }
    throw new IllegalArgumentException("Unsupported block file path: " + path);
  }

  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  protected final String FILE_PATH;

  protected BlockHandler(String path) {
    FILE_PATH = Preconditions.checkNotNull(path);
  }

  /**
   * Write data into block file
   * 
   * @param buf
   *          buffer that data is stored in
   * @param inFilePos
   *          starting position of the file
   * @param offset
   *          offset of the buf
   * @param length
   *          length of the data
   * @return size of data that is written
   * @throws IOException
   */
  public abstract int appendCurrentBuffer(byte[] buf, long inFilePos, int offset, int length)
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
   * @param offset
   *          offset from starting of the file
   * @param length
   *          length of data to read, -1 represents reading the rest of the block file
   * @return byte buffer storing data that is read
   * @throws IOException
   */
  public abstract ByteBuffer readByteBuffer(long offset, int length) throws IOException;
}
