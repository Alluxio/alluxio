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
import alluxio.network.protocol.databuffer.DataNettyBuffer;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents the response of a {@link RPCFileReadRequest}.
 */
@ThreadSafe
public final class RPCFileReadResponse extends RPCResponse {
  private final long mTempUfsFileId;
  private final long mOffset;
  private final long mLength;
  private final DataBuffer mData;
  private final Status mStatus;

  /**
   * Constructs a new RPC response of a {@link RPCFileReadRequest}.
   *
   * @param tempUfsFileId the worker specific id of the ufs file to read
   * @param offset the file offset that the read began at
   * @param length the number of bytes read
   * @param data the data for the response
   * @param status the status of the response
   */
  public RPCFileReadResponse(long tempUfsFileId, long offset, long length, DataBuffer data,
      Status status) {
    mTempUfsFileId = tempUfsFileId;
    mOffset = offset;
    mLength = length;
    mData = data;
    mStatus = status;
  }

  @Override
  public Type getType() {
    return Type.RPC_FILE_READ_RESPONSE;
  }

  /**
   * Creates a {@link RPCFileReadResponse} object that indicates an error for the given
   * {@link RPCFileReadRequest}.
   *
   * @param request the {@link RPCFileReadRequest} to generate the {@link RPCFileReadResponse} for
   * @param status the {@link alluxio.network.protocol.RPCResponse.Status} of the response
   * @return The generated {@link RPCFileReadResponse} object
   */
  public static RPCFileReadResponse createErrorResponse(final RPCFileReadRequest request,
      final Status status) {
    Preconditions.checkArgument(status != Status.SUCCESS);
    // The response has no payload, so length must be 0.
    return
        new RPCFileReadResponse(request.getTempUfsFileId(), request.getOffset(), 0, null, status);
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCFileReadResponse} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCFileReadResponse object
   */
  public static RPCFileReadResponse decode(ByteBuf in) {
    long tempUfsFileId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    short status = in.readShort();

    DataBuffer data = null;
    if (length > 0) {
      data = new DataNettyBuffer(in, (int) length);
    }
    return new RPCFileReadResponse(tempUfsFileId, offset, length, data, Status.fromShort(status));
  }

  @Override
  public int getEncodedLength() {
    // 3 longs (mTempUfsFileId, mOffset, mLength) + 1 short (mStatus)
    return Longs.BYTES * 3 + Shorts.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mTempUfsFileId);
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
    return Objects.toStringHelper(this).add("tempUfsFileId", mTempUfsFileId).add("offset", mOffset)
        .add("length", mLength).add("status", mStatus).toString();
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset cannot be negative: %s", mOffset);
    Preconditions.checkState(mLength >= 0, "Length cannot be negative: %s", mLength);
  }

  /**
   * @return the worker specific id of the ufs file
   */
  public long getTempUfsFileId() {
    return mTempUfsFileId;
  }

  /**
   * @return the number of bytes read
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the file offset that the read began at
   */
  public long getOffset() {
    return mOffset;
  }

  @Override
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return if the message indicates the reader has reached the end of the file
   */
  public boolean isEOF() {
    return mLength == 0;
  }
}
