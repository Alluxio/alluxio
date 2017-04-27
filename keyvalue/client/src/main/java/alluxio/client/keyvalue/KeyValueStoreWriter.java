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

import alluxio.annotation.PublicApi;
import alluxio.client.Cancelable;
import alluxio.exception.AlluxioException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for writers which create new key-value stores in Alluxio.
 */
@PublicApi
public interface KeyValueStoreWriter extends Closeable, Cancelable {
  /**
   * Adds a key and its associated value to this store.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   */
  void put(byte[] key, byte[] value) throws IOException, AlluxioException;

  /**
   * Adds a key and its associated value to this store.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   */
  void put(ByteBuffer key, ByteBuffer value) throws IOException, AlluxioException;

}
