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
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  void put(byte[] key, byte[] value) throws IOException, AlluxioException;

  /**
   * Adds a key and its associated value to this store.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  void put(ByteBuffer key, ByteBuffer value) throws IOException, AlluxioException;

}
