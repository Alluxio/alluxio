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
