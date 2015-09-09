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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBuf;

/**
 * This represents an RPC request to read a block from a DataServer.
 */
public final class RPCBlockReadRequest extends RPCRequest {
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;

  public RPCBlockReadRequest(long blockId, long offset, long length) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
  }

  @Override
  public Type getType() {
    return Type.RPC_BLOCK_READ_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCBlockReadRequest} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockReadRequest object
   */
  public static RPCBlockReadRequest decode(ByteBuf in) {
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    return new RPCBlockReadRequest(blockId, offset, length);
  }

  @Override
  public int getEncodedLength() {
    // 3 longs (mBLockId, mOffset, mLength)
    return Longs.BYTES * 3;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset cannot be negative: %s", mOffset);
    Preconditions.checkState(mLength >= 0 || mLength == -1,
        "Length cannot be negative (except for -1): %s", mLength);
  }

  @Override
  public String toString() {
    return "RPCBlockReadRequest(" + mBlockId + ", " + mOffset + ", " + mLength + ")";
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
