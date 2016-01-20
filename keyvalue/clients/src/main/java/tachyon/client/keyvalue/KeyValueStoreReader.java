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

import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;

/**
 * Interface for readers which accesses key-value stores in Tachyon.
 */
@PublicApi
public interface KeyValueStoreReader extends Closeable {
  /**
   * Gets the value associated with {@code key}, returns null if not found. When getting large
   * values (e.g., larger than 10KB), {@link #get(ByteBuffer)} might be more efficient by taking
   * advantage from zero-copy.
   *
   * @param key key to get, cannot be null
   * @return value associated with the given key, or null if not found
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  byte[] get(byte[] key) throws IOException, TachyonException;

  /**
   * Gets the value associated with {@code key}, returns null if not found.
   *
   * @param key key to get, cannot be null
   * @return value associated with the given key, or null if not found
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  ByteBuffer get(ByteBuffer key) throws IOException, TachyonException;

}
