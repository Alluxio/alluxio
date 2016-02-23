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

import alluxio.annotation.PublicApi;
import alluxio.exception.AlluxioException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for readers which accesses key-value stores in Alluxio.
 */
@PublicApi
public interface KeyValueStoreReader extends Closeable, KeyValueIterable {
  /**
   * Gets the value associated with {@code key}, returns null if not found. When getting large
   * values (e.g., larger than 10KB), {@link #get(ByteBuffer)} might be more efficient by taking
   * advantage from zero-copy.
   *
   * @param key key to get, cannot be null
   * @return value associated with the given key, or null if not found
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  byte[] get(byte[] key) throws IOException, AlluxioException;

  /**
   * Gets the value associated with {@code key}, returns null if not found.
   *
   * @param key key to get, cannot be null
   * @return value associated with the given key, or null if not found
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException;

  /**
   * @return the number of key-value pairs in the store
   * @throws IOException if a non-Alluxio error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  int size() throws IOException, AlluxioException;
}
