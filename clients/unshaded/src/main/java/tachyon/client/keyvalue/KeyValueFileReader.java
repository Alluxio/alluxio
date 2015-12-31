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

package tachyon.client.keyvalue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.TachyonException;

/**
 * Interface of the reader class to access a Tachyon key-value file.
 */
public interface KeyValueFileReader extends Closeable {

  class Factory {
    /**
     * Factory method to create a {@link KeyValueFileReader} given TachyonURI.
     *
     * @param uri Tachyon URI of the key-value file to use as input
     * @return an instance of a {@link KeyValueFileReader}
     * @throws IOException if a non-Tachyon exception occurs
     * @throws TachyonException if an unexpected tachyon exception is thrown
     */
    public static KeyValueFileReader create(TachyonURI uri) throws TachyonException, IOException {
      Preconditions.checkNotNull(uri);
      TachyonFileSystem tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
      TachyonFile tFile = tfs.open(uri);
      List<Long> blockIds = tfs.getInfo(tFile).getBlockIds();
      long blockId = blockIds.get(0);
      return new BaseKeyValueFileReader(blockId);
    }

    /**
     * Factory method to create a {@link KeyValueFileReader} given blockId.
     *
     * @param blockId blockId the key-value file to use as input
     * @return an instance of a {@link KeyValueFileReader}
     * @throws IOException if a non-Tachyon exception occurs
     * @throws TachyonException if an unexpected tachyon exception is thrown
     */
    public static KeyValueFileReader create(long blockId) throws TachyonException, IOException {
      return new BaseKeyValueFileReader(blockId);
    }
  }

  /**
   * Gets the value associated with the given key in the key-value file, returning null if the key
   * is not found.
   *
   * @param key key to get, cannot be null
   * @return bytes of the value if found, null otherwise
   */
  byte[] get(byte[] key) throws IOException, TachyonException;

  /**
   * Gets the value associated with the given key in the key-value file, returning null if the key
   * is not found. Both key and value are in ByteBuffer to make zero-copy possible for better
   * performance.
   *
   * @param key key to get, cannot be null
   * @return bytes of the value if found, null otherwise
   */
  ByteBuffer get(ByteBuffer key) throws IOException, TachyonException;


  /**
   * Closes this reader.
   */
  void close() throws IOException;
}
