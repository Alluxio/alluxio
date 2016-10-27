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

import com.google.common.base.Objects;
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

  @Override
  public Status getStatus() {
    return mStatus;
  }

  @Override
  public Type getType() {
    return Type.RPC_ERROR_RESPONSE;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("status", mStatus).toString();
  }
}
