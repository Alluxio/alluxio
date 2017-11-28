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

package alluxio.client.keyvalue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Interface for a reader which accesses an Alluxio key-value partition.
 */
public interface KeyValuePartitionReader extends Closeable, KeyValueIterable {

  /**
   * Factory for {@link KeyValuePartitionReader}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method to create a {@link KeyValuePartitionReader} given the {@link AlluxioURI} of a
     * key-value partition.
     *
     * @param uri Alluxio URI of the key-value partition to use as input
     * @return an instance of a {@link KeyValuePartitionReader}
     */
    public static KeyValuePartitionReader create(AlluxioURI uri)
        throws AlluxioException, IOException {
      Preconditions.checkNotNull(uri);
      FileSystem fs = FileSystem.Factory.get();
      List<Long> blockIds = fs.getStatus(uri).getBlockIds();
      // Each partition file should only contain one block.
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
     */
    public static KeyValuePartitionReader create(long blockId)
        throws AlluxioException, IOException {
      return new BaseKeyValuePartitionReader(blockId);
    }
  }

  /**
   * Gets the value associated with the given key in the key-value partition, returning null if the
   * key is not found.
   *
   * @param key key to get, cannot be null
   * @return bytes of the value if found, null otherwise
   */
  byte[] get(byte[] key) throws IOException, AlluxioException;

  /**
   * Gets the value associated with the given key in the key-value partition, returning null if the
   * key is not found. Both key and value are in ByteBuffer to make zero-copy possible for better
   * performance.
   *
   * @param key key to get, cannot be null
   * @return bytes of the value if found, null otherwise
   */
  ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException;

  /**
   * @return the number of key-value pairs in the partition
   */
  int size() throws IOException, AlluxioException;
}
