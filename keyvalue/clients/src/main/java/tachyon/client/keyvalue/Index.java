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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface of key-value index. An implementation of this interface is supposed to index keys
 * and capable of returning value of a given key from payload buffer.
 */
public interface Index {

  /**
   * Puts a key into the index and store key and value data into payload storage.
   *
   * @param key bytes of key
   * @param value bytes of value
   * @param writer writer to store key-value payload
   * @return true on success, false otherwise (e.g., unresolvable hash collision)
   * @throws IOException if errors happen when writer writes key/value
   */
  boolean put(byte[] key, byte[] value, PayloadWriter writer) throws IOException;

  /**
   * Gets the bytes of value given the key and payload storage reader.
   *
   * @param key bytes of key
   * @param reader reader to access key-value payload
   * @return ByteBuffer of value, or null if not found
   */
  ByteBuffer get(ByteBuffer key, PayloadReader reader) throws IOException;

  /**
   * @return byte array of this index
   */
  byte[] getBytes();

  /**
   * @return size of this index in bytes
   */
  int byteCount();

  /**
   * @return the number of keys inserted
   */
  int keyCount();

}
