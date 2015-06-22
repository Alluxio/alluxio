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

import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBuf;

import tachyon.network.protocol.databuffer.DataBuffer;
import tachyon.network.protocol.databuffer.DataByteBuffer;

/**
 * This represents the response to a {@link RPCBlockWriteRequest}.
 */
public class RPCBlockWriteResponse extends RPCResponse {
  private final long mUserId;
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;

  public RPCBlockWriteResponse(long userId, long blockId, long offset, long length) {
    mUserId = userId;
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
  }

  public Type getType() {
    return Type.RPC_BLOCK_WRITE_RESPONSE;
  }

  /**
   * Decode the input {@link ByteBuf} into a {@link RPCBlockWriteResponse} object and return it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockResponse object
   */
  public static RPCBlockWriteResponse decode(ByteBuf in) {
    long userId = in.readLong();
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    return new RPCBlockWriteResponse(userId, blockId, offset, length);
  }

  @Override
  public int getEncodedLength() {
    // 4 longs (mUserId, mBLockId, mOffset, mLength)
    return Longs.BYTES * 4;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mUserId);
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
  }

  public long getUserId() {
    return mUserId;
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
}
