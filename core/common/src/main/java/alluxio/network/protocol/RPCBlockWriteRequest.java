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

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents the request to write a block to a DataServer.
 */
@ThreadSafe
public final class RPCBlockWriteRequest extends RPCRequest {
  private final long mSessionId;
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final DataBuffer mData;

  /**
   * Constructs a new request to write a block to a DataServer.
   *
   * @param sessionId the id of the session
   * @param blockId the id of the block
   * @param offset the block offset to begin writing at
   * @param length the number of bytes to write
   * @param data the data
   */
  public RPCBlockWriteRequest(long sessionId, long blockId, long offset, long length,
      DataBuffer data) {
    mSessionId = sessionId;
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mData = data;
  }

  @Override
  public Type getType() {
    return Type.RPC_BLOCK_WRITE_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCBlockWriteRequest} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockWriteRequest object
   */
  public static RPCBlockWriteRequest decode(ByteBuf in) {
    long sessionId = in.readLong();
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    // TODO(gene): Look into accessing Netty ByteBuf directly, to avoid copying the data.
    // Length will always be greater than 0 if the request is not corrupted. If length is negative,
    // ByteBuffer.allocate will fail. If length is 0 this will become a no-op but still go through
    // the necessary calls to validate the sessionId/blockId. If length is positive, the request
    // will proceed as normal
    ByteBuffer buffer = ByteBuffer.allocate((int) length);
    in.readBytes(buffer);
    DataByteBuffer data = new DataByteBuffer(buffer, (int) length);
    return new RPCBlockWriteRequest(sessionId, blockId, offset, length, data);
  }

  @Override
  public int getEncodedLength() {
    // 4 longs (mSessionId, mBlockId, mOffset, mLength)
    return Longs.BYTES * 4;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mSessionId);
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
   * @return the number of bytes to write
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the block offset to begin writing at
   */
  public long getOffset() {
    return mOffset;
  }
}
