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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents an RPC request to read a UFS block from a DataServer.
 */
@ThreadSafe
public final class RPCUnderFileSystemBlockReadRequest extends RPCRequest {
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final long mSessionId;
  private final boolean mNoCache;

  /**
   * Constructs a new RPC request to read a block from a DataServer.
   *
   * @param blockId the id of the block
   * @param offset the block offset to begin reading at
   * @param length the number of bytes to read
   * @param sessionId the id of the client session
   * @param noCache do not cache the block if set
   */
  public RPCUnderFileSystemBlockReadRequest(long blockId, long offset, long length, long sessionId,
      boolean noCache) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mSessionId = sessionId;
    mNoCache = noCache;
  }

  @Override
  public Type getType() {
    return Type.RPC_UFS_BLOCK_READ_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCUnderFileSystemBlockReadRequest} object
   * and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCBlockReadRequest object
   */
  public static RPCUnderFileSystemBlockReadRequest decode(ByteBuf in) {
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    long sessionId = in.readLong();
    boolean noCache = in.readBoolean();
    return new RPCUnderFileSystemBlockReadRequest(blockId, offset, length, sessionId, noCache);
  }

  @Override
  public int getEncodedLength() {
    // 4 longs (mBLockId, mOffset, mLength, mSessionId) and 1 boolean (boolean takes 1 byte in
    // netty).
    return Longs.BYTES * 4 + 1;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mBlockId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
    out.writeLong(mSessionId);
    out.writeBoolean(mNoCache);
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset cannot be negative: %s", mOffset);
    Preconditions
        .checkState(mLength >= 0, "Length cannot be negative (except for -1): %s", mLength);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("blockId", mBlockId).add("offset", mOffset)
        .add("length", mLength).add("sessionId", mSessionId).add("noCache", mNoCache).toString();
  }

  /**
   * @return the id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the number of bytes to read
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the block offset to begin reading at
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the id of the session
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return true if noCache is set
   */
  public boolean getNoCache() {
    return mNoCache;
  }
}
