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

package alluxio.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Represents an encoded message.
 */
public interface EncodedMessage {

  /**
   * Returns the number bytes for the message when it is encoded.
   *
   * @return the length of the encoded message, in bytes
   */
  int getEncodedLength();

  /**
   * Encodes the message to the output {@link ByteBuf}.
   *
   * @param out the output ByteBuf where the message should be encoded
   */
  void encode(ByteBuf out);
}
