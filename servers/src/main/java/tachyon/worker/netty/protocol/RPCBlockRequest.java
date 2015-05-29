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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;

import tachyon.Constants;
import tachyon.worker.DataServerMessage;

/**
 * This represents an RPC request to read a block from a DataServer.
 */
public class RPCBlockRequest extends RPCRequest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final long mBlockId;
  private final long mOffset;
  private final long mLength;

  private RPCBlockRequest(long blockId, long offset, long length) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
  }

  public Type getType() {
    return Type.RPC_BLOCK_REQUEST;
  }

  /**
   * Decode the input {@link ByteBuf} into a {@link RPCBlockRequest} object and return it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockRequest object
   */
  public static RPCBlockRequest decode(ByteBuf in) {
    // TODO: remove this short when client also uses netty.
    in.readShort();
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    return new RPCBlockRequest(blockId, offset, length);
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
    out.writeShort(DataServerMessage.DATA_SERVER_REQUEST_MESSAGE);
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset can not be negative: %s", mOffset);
    Preconditions.checkState(mLength >= 0 || mLength == -1,
        "Length can not be negative except -1: %s", mLength);
  }

  @Override
  public String toString() {
    return "RPCBlockRequest(" + mBlockId + ", " + mOffset + ", " + mLength + ")";
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
