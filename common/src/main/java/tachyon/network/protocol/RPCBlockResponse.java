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

package tachyon.network.protocol;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;

import tachyon.network.protocol.databuffer.DataBuffer;
import tachyon.network.protocol.databuffer.DataByteBuffer;

/**
 * This represents the response of a {@link RPCBlockRequest}.
 */
public class RPCBlockResponse extends RPCResponse {
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final DataBuffer mData;
  private final Status mStatus;

  // TODO: rename this to RPCBlockReadResponse.
  public RPCBlockResponse(long blockId, long offset, long length, DataBuffer data, Status status) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mData = data;
    mStatus = status;
  }

  public Type getType() {
    return Type.RPC_BLOCK_RESPONSE;
  }

  /**
   * Creates a {@link RPCBlockResponse} object that indicates an error for the given
   * {@link RPCBlockRequest}.
   *
   * @param request The {@link RPCBlockRequest} to generated the {@link RPCBlockResponse} for.
   * @param status The {@link tachyon.network.protocol.RPCResponse.Status} for the response.
   * @return The generated {@link RPCBlockResponse} object.
   */
  public static RPCBlockResponse createErrorResponse(final RPCBlockRequest request,
      final Status status) {
    Preconditions.checkArgument(status != Status.SUCCESS);
    // The response has no payload, so length must be 0.
    return new RPCBlockResponse(request.getBlockId(), request.getOffset(), 0, null, status);
  }

  /**
   * Decode the input {@link ByteBuf} into a {@link RPCBlockResponse} object and return it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockResponse object
   */
  public static RPCBlockResponse decode(ByteBuf in) {
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    short status = in.readShort();
    DataBuffer data = null;
    if (length > 0) {
      // TODO: look into accessing Netty ByteBuf directly, to avoid copying the data.
      ByteBuffer buffer = ByteBuffer.allocate((int) length);
      in.readBytes(buffer);
      data = new DataByteBuffer(buffer, (int) length);
    }
    return new RPCBlockResponse(blockId, offset, length, data, Status.fromShort(status));
  }

  @Override
  public int getEncodedLength() {
    // 3 longs (mBLockId, mOffset, mLength) + 1 short (mStatus)
    return Longs.BYTES * 3 + Shorts.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
    out.writeShort(mStatus.getId());
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mData;
  }

  @Override
  public String toString() {
    return "RPCBlockResponse(" + mBlockId + ", " + mOffset + ", " + mLength + ", " + mStatus + ")";
  }

  public long getBlockId() {
    return mBlockId;
  }

  public long getLength() {
    return mLength;
  }

  public long getOffset() {
    return mOffset;
  }

  public Status getStatus() {
    return mStatus;
  }
}
