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

import com.google.common.primitives.Shorts;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents a simple RPC response, containing an error.
 */
@ThreadSafe
public final class RPCErrorResponse extends RPCResponse {
  private final Status mStatus;

  /**
   * Constructs a new RPC response, containing an error.
   *
   * @param status the status
   */
  public RPCErrorResponse(Status status) {
    mStatus = status;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCErrorResponse} object and returns it.
   *
   * @param in The input {@link ByteBuf}
   * @return The decoded RPCErrorResponse object
   */
  public static RPCErrorResponse decode(ByteBuf in) {
    return new RPCErrorResponse(Status.fromShort(in.readShort()));
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeShort(mStatus.getId());
  }

  @Override
  public int getEncodedLength() {
    // 1 short (mStatus)
    return Shorts.BYTES;
  }

  /**
   * @return the status
   */
  public Status getStatus() {
    return mStatus;
  }

  @Override
  public Type getType() {
    return Type.RPC_ERROR_RESPONSE;
  }

  @Override
  public String toString() {
    return "RPCErrorResponse(" + mStatus + ")";
  }
}
