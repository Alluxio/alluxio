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

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents the request to write a file to the ufs through an Alluxio Worker. The file
 * must have been previously created through an RPC call.
 */
@ThreadSafe
public final class RPCFileWriteRequest extends RPCRequest {
  private final long mTempUfsFileId;
  private final long mOffset;
  private final long mLength;
  private final DataBuffer mData;

  /**
   * Constructs a new request to write to a ufs file through an Alluxio Worker.
   *
   * @param tempUfsFileId the worker specific ufs file id
   * @param offset the file offset to begin writing at
   * @param length the number of bytes to write
   * @param data the data
   */
  public RPCFileWriteRequest(long tempUfsFileId, long offset, long length, DataBuffer data) {
    mTempUfsFileId = tempUfsFileId;
    mOffset = offset;
    mLength = length;
    mData = data;
  }

  @Override
  public Type getType() {
    return Type.RPC_FILE_WRITE_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCFileWriteRequest} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCFileWriteRequest object
   */
  public static RPCFileWriteRequest decode(ByteBuf in) {
    long tempUfsFileId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    // TODO(gene): Look into accessing Netty ByteBuf directly, to avoid copying the data.
    // Length will always be greater than 0 if the request is not corrupted. If length is negative,
    // ByteBuffer.allocate will fail. If length is 0 this will become a no-op but still go through
    // the necessary calls to validate the tempUfsFileId. If length is positive, the request will
    // proceed as normal
    ByteBuffer buffer = ByteBuffer.allocate((int) length);
    in.readBytes(buffer);
    DataByteBuffer data = new DataByteBuffer(buffer, (int) length);
    return new RPCFileWriteRequest(tempUfsFileId, offset, length, data);
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
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mData;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("tempUfsFileId", mTempUfsFileId).add("offset", mOffset)
        .add("length", mLength).toString();
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset cannot be negative: %s", mOffset);
    Preconditions.checkState(mLength >= 0, "Length cannot be negative: %s", mLength);
  }

  /**
   * @return the worker specific ufs file id
   */
  public long getTempUfsFileId() {
    return mTempUfsFileId;
  }

  /**
   * @return the number of bytes to write
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the file offset to begin writing at
   */
  public long getOffset() {
    return mOffset;
  }
}
