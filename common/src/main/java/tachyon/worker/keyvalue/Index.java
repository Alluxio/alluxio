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

package tachyon.worker.keyvalue;

import java.io.IOException;

/**
 * Interface of key-value Index.
 */
public interface Index {

  /**
   * Puts a key into the index, associated with an offset into the key-value payload.
   *
   * @param key bytes of key
   * @param offset offset of this key in payload
   * @return true on success, false otherwise
   */
  boolean put(byte[] key, int offset) throws IOException;

  /**
   * Gets the bytes of value given the key
   *
   * @param key bytes of key
   * @param payload the byte array of all key value payload
   * @return bytes of value, or null if not found
   */
  byte[] get(byte[] key, PayloadReader reader) throws IOException ;

  /**
   * @return size of this index in bytes.
   */
  int byteCount();

  /**
   * @return size of this index in bytes.
   */
  int keyCount();

  /**
   * @return byte array of this index.
   */
  byte[] toByteArray();
}
