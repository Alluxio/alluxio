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
 * Interface to write key and value to payload storage.
 */
public interface PayloadWriter {

  /**
   * Puts a key into payload, return the offset
   *
   * @param key bytes of key
   * @param value offset of this key in payload
   * @return the offset of this key-value pair in payload storage
   * @throws IOException if error occurs writing the key and value to output stream
   */
  int addKeyAndValue(byte[] key, byte[] value) throws IOException;
}
