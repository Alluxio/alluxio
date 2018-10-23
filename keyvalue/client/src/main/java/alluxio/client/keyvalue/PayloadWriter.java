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

import java.io.IOException;

/**
 * Interface to write to the payload buffer.
 */
public interface PayloadWriter {

  /**
   * Inserts a key and a value into the payload buffer, returns an offset indicating where the key
   * and value data is stored in payload buffer.
   *
   * @param key bytes of key
   * @param value bytes of value
   * @return the offset of this key-value pair in payload buffer
   */
  int insert(byte[] key, byte[] value) throws IOException;
}
