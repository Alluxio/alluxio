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
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;

import tachyon.network.protocol.databuffer.DataBuffer;
import tachyon.network.protocol.databuffer.DataByteBuffer;
import tachyon.worker.DataServerMessage;

/**
 * This represents the request to write a block to a DataServer.
 */
public class RPCBlockWriteRequest extends RPCRequest {
  private final long mUserId;
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final DataBuffer mData;

  public RPCBlockWriteRequest(long userId, long blockId, long offset, long length,
                              DataBuffer data) {
    mUserId = userId;
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mData = data;
  }

  public Type getType() {
    return Type.RPC_BLOCK_WRITE_REQUEST;
  }

  /**
   * Decode the input {@link ByteBuf} into a {@link RPCBlockWriteRequest} object and return it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockResponse object
   */
  public static RPCBlockWriteRequest decode(ByteBuf in) {
    long userId = in.readLong();
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    DataBuffer data = null;
    if (length > 0) {
      // TODO: look into accessing Netty ByteBuf directly, to avoid copying the data.
      ByteBuffer buffer = ByteBuffer.allocate((int) length);
      in.readBytes(buffer);
      data = new DataByteBuffer(buffer, (int) length);
    }
    return new RPCBlockWriteRequest(userId, blockId, offset, length, data);
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
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mData;
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
