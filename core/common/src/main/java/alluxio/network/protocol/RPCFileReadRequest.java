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
 * This represents an RPC request to read a file from an under file system from an Alluxio worker.
 * The file must have been previously opened through an RPC call.
 */
@ThreadSafe
public final class RPCFileReadRequest extends RPCRequest {
  private final long mTempUfsFileId;
  private final long mOffset;
  private final long mLength;

  /**
   * Constructs a new RPC request to read a file from an under file system from an Alluxio worker.
   *
   * @param tempUfsFileId the worker specific id of the ufs file to read
   * @param offset the relative file offset to begin reading at
   * @param length the number of bytes to read
   */
  public RPCFileReadRequest(long tempUfsFileId, long offset, long length) {
    mTempUfsFileId = tempUfsFileId;
    mOffset = offset;
    mLength = length;
  }

  @Override
  public Type getType() {
    return Type.RPC_FILE_READ_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCFileReadRequest} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCFileReadRequest object
   */
  public static RPCFileReadRequest decode(ByteBuf in) {
    long tempUfsFileId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    return new RPCFileReadRequest(tempUfsFileId, offset, length);
  }

  @Override
  public int getEncodedLength() {
    // 3 longs (mTempUfsFileId, mOffset, mLength)
    return Longs.BYTES * 3;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mTempUfsFileId);
    out.writeLong(mOffset);
    out.writeLong(mLength);
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset cannot be negative: %s", mOffset);
    Preconditions.checkState(mLength >= 0, "Length cannot be negative: %s", mLength);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("tempUfsFileId", mTempUfsFileId).add("offset", mOffset)
        .add("length", mLength).toString();
  }

  /**
   * @return the temporary under file system file id
   */
  public long getTempUfsFileId() {
    return mTempUfsFileId;
  }

  /**
   * @return the number of bytes to read
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the file offset to begin reading at
   */
  public long getOffset() {
    return mOffset;
  }
}
