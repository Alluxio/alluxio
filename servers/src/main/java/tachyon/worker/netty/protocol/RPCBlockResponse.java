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

package tachyon.worker.netty.protocol;

import java.nio.ByteBuffer;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;

import tachyon.worker.DataServerMessage;
import tachyon.worker.netty.protocol.buffer.DataBuffer;
import tachyon.worker.netty.protocol.buffer.DataByteBuffer;

/**
 * This represents the response of a {@link RPCBlockRequest}.
 */
public class RPCBlockResponse extends RPCResponse {
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final DataBuffer mData;

  public RPCBlockResponse(long blockId, long offset, long length, DataBuffer data) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mData = data;
  }

  public Type getType() {
    return Type.RPC_BLOCK_RESPONSE;
  }

  /**
   * Creates a {@link tachyon.worker.netty.protocol.RPCBlockResponse} that indicates an error for
   * the given block.
   *
   * @param blockId The Id of block requested
   * @return the new error RPCBlockResponse created.
   */
  public static RPCBlockResponse createErrorResponse(final long blockId) {
    return new RPCBlockResponse(-blockId, 0, 0, null);
  }

  /**
   * Decode the input {@link ByteBuf} into a {@link RPCBlockResponse} object and return it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockResponse object
   */
  public static RPCBlockResponse decode(ByteBuf in) {
    // TODO: remove this short when client also uses netty.
    in.readShort();
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    DataBuffer data = null;
    if (length > 0) {
      ByteBuffer buffer = ByteBuffer.allocate((int) length);
      in.readBytes(buffer);
      data = new DataByteBuffer(buffer, (int) length);
    }
    return new RPCBlockResponse(blockId, offset, length, data);
  }

  @Override
  public int getEncodedLength() {
    // TODO: adjust the length when client also uses netty.
    // 3 longs (mBLockId, mOffset, mLength) + 1 short (DATA_SERVER_REQUEST_MESSAGE)
    return Longs.BYTES * 3 + Shorts.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    // TODO: remove this short when client also uses netty.
    out.writeShort(DataServerMessage.DATA_SERVER_RESPONSE_MESSAGE);
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mData;
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
