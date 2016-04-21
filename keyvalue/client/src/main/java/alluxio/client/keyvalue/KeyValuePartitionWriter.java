/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.Cancelable;
import alluxio.client.ClientContext;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for a writer which creates an Alluxio key-value partition.
 */
public interface KeyValuePartitionWriter extends Closeable, Cancelable {

  /**
   * Factory for {@link KeyValuePartitionWriter}.
   */
  class Factory {
    /**
     * Factory method to create a {@link KeyValuePartitionWriter} instance that writes key-value
     * data to a new partition file in Alluxio.
     *
     * @param uri URI of the key-value partition file to write to
     * @return an instance of a {@link KeyValuePartitionWriter}
     * @throws IOException if a non-Alluxio exception occurs
     * @throws AlluxioException if an unexpected Alluxio exception is thrown
     */
    public static KeyValuePartitionWriter create(AlluxioURI uri)
        throws AlluxioException, IOException {
      Preconditions.checkNotNull(uri);
      FileSystem fs = FileSystem.Factory.get();
      CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(
          ClientContext.getConf().getBytes(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX));
      FileOutStream fileOutStream = fs.createFile(uri, options);
      return new BaseKeyValuePartitionWriter(fileOutStream);
    }
  }

  /**
   * Adds a key and the associated value to this writer.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   * @throws IOException if a non-Alluxio exception occurs
   */
  void put(byte[] key, byte[] value) throws IOException;

  /**
   * Returns whether this writer will be full after inserting the given key-value pair.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   * @return whether this writer is full to take any more key-value pairs
   */
  boolean canPut(byte[] key, byte[] value);
}
