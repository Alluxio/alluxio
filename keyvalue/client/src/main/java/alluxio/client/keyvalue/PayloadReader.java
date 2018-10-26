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

import java.nio.ByteBuffer;

/**
 * Interface to access keys and values from the payload buffer.
 */
public interface PayloadReader {

  /**
   * Gets the key given the position of payload buffer.
   *
   * @param pos position in the payload storage in bytes
   * @return key in {@code ByteBuffer}
   */
  ByteBuffer getKey(int pos);

  /**
   * Gets the value given the position of payload buffer.
   *
   * @param pos position in the payload storage in bytes
   * @return value in {@code ByteBuffer}
   */
  ByteBuffer getValue(int pos);
}
