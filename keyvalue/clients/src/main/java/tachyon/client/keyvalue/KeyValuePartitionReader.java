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
 * Interface for a reader which accesses a Tachyon key-value partition.
 */
public interface KeyValuePartitionReader extends Closeable, KeyValueIterable {

  class Factory {
    /**
     * Factory method to create a {@link KeyValuePartitionReader} given the {@link TachyonURI} of a
     * key-value partition.
     *
     * @param uri Tachyon URI of the key-value partition to use as input
     * @return an instance of a {@link KeyValuePartitionReader}
     * @throws IOException if a non-Tachyon exception occurs
     * @throws TachyonException if an unexpected Tachyon exception is thrown
     */
    public static KeyValuePartitionReader create(TachyonURI uri)
        throws TachyonException, IOException {
      Preconditions.checkNotNull(uri);
      TachyonFileSystem tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
      TachyonFile tFile = tfs.open(uri);
      List<Long> blockIds = tfs.getInfo(tFile).getBlockIds();
      // Each partition file should only contains one block.
      // TODO(binfan): throw exception if a partition file has more than one blocks
      long blockId = blockIds.get(0);
      return new BaseKeyValuePartitionReader(blockId);
    }

    /**
     * Factory method to create a {@link KeyValuePartitionReader} given the block id of a key-value
     * partition.
     *
     * @param blockId blockId the key-value partition to use as input
     * @return an instance of a {@link KeyValuePartitionReader}
     * @throws IOException if a non-Tachyon exception occurs
     * @throws TachyonException if an unexpected Tachyon exception is thrown
     */
    public static KeyValuePartitionReader create(long blockId)
        throws TachyonException, IOException {
      return new BaseKeyValuePartitionReader(blockId);
    }
  }

  /**
   * Gets the value associated with the given key in the key-value partition, returning null if the
   * key is not found.
   *
   * @param key key to get, cannot be null
   * @return bytes of the value if found, null otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  byte[] get(byte[] key) throws IOException, TachyonException;

  /**
   * Gets the value associated with the given key in the key-value partition, returning null if the
   * key is not found. Both key and value are in ByteBuffer to make zero-copy possible for better
   * performance.
   *
   * @param key key to get, cannot be null
   * @return bytes of the value if found, null otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  ByteBuffer get(ByteBuffer key) throws IOException, TachyonException;

  /**
   * @return number of key-value pairs in the partition
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  int size() throws IOException, TachyonException;
}
