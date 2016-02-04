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

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;

/**
 * This represents the response to a {@link RPCBlockWriteRequest}.
 */
@ThreadSafe
public final class RPCBlockWriteResponse extends RPCResponse {
  private final long mSessionId;
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final Status mStatus;

  /**
   * Constructs a new response to a {@link RPCBlockWriteRequest}.
   *
   * @param sessionId the id of the session
   * @param blockId the id of the block
   * @param offset the block offset that the writing began at
   * @param length the number of bytes written
   * @param status the status
   */
  public RPCBlockWriteResponse(long sessionId, long blockId, long offset, long length,
      Status status) {
    mSessionId = sessionId;
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mStatus = status;
  }

  /**
   * Creates a {@link RPCBlockWriteResponse} object that indicates an error for the given
   * {@link RPCBlockWriteRequest}.
   *
   * @param request the {@link RPCBlockWriteRequest} to generated the {@link RPCBlockReadResponse}
   *        for.
   * @param status the {@link alluxio.network.protocol.RPCResponse.Status} for the response
   * @return The generated {@link RPCBlockWriteResponse} object
   */
  public static RPCBlockWriteResponse createErrorResponse(final RPCBlockWriteRequest request,
      final Status status) {
    Preconditions.checkArgument(status != Status.SUCCESS);
    // The response has no payload, so length must be 0.
    return new RPCBlockWriteResponse(request.getSessionId(), request.getBlockId(),
        request.getOffset(), request.getLength(), status);
  }

  @Override
  public Type getType() {
    return Type.RPC_BLOCK_WRITE_RESPONSE;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCBlockWriteResponse} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return the decoded RPCBlockWriteResponse object
   */
  public static RPCBlockWriteResponse decode(ByteBuf in) {
    long sessionId = in.readLong();
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    short status = in.readShort();
    return new RPCBlockWriteResponse(sessionId, blockId, offset, length, Status.fromShort(status));
  }

  @Override
  public int getEncodedLength() {
    // 4 longs (mSessionId, mBlockId, mOffset, mLength) + 1 short (mStatus)
    return Longs.BYTES * 4 + Shorts.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mSessionId);
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
    out.writeShort(mStatus.getId());
  }

  /**
   * @return the id of the session
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return the id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the number of bytes written
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the block offset that the writing began at
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the status
   */
  public Status getStatus() {
    return mStatus;
  }
}
