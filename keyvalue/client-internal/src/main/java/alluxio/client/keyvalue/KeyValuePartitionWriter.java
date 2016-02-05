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

package alluxio.client.keyvalue;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Preconditions;

import alluxio.AlluxioURI;
import alluxio.client.Cancelable;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

/**
 * Interface for a writer which creates a Alluxio key-value partition.
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
      FileOutStream fileOutStream = fs.createFile(uri);
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
  // TODO(binfan): throw already exists exception if the key is already inserted.
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
