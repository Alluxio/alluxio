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
import com.google.common.primitives.Shorts;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents the response to a {@link RPCFileWriteRequest}.
 */
@ThreadSafe
public final class RPCFileWriteResponse extends RPCResponse {
  private final long mTempUfsFileId;
  private final long mOffset;
  private final long mLength;
  private final Status mStatus;

  /**
   * Constructs a new response to a {@link RPCFileWriteRequest}.
   *
   * @param tempUfsFileId the worker specific ufs file id
   * @param offset the file offset that the writing began at
   * @param length the number of bytes written
   * @param status the status
   */
  public RPCFileWriteResponse(long tempUfsFileId, long offset, long length, Status status) {
    mTempUfsFileId = tempUfsFileId;
    mOffset = offset;
    mLength = length;
    mStatus = status;
  }

  /**
   * Creates a {@link RPCFileWriteResponse} object that indicates an error for the given
   * {@link RPCFileWriteRequest}.
   *
   * @param request the {@link RPCFileWriteRequest} to generated the {@link RPCFileWriteResponse}
   *        for.
   * @param status the {@link alluxio.network.protocol.RPCResponse.Status} for the response
   * @return The generated {@link RPCFileWriteResponse} object
   */
  public static RPCFileWriteResponse createErrorResponse(final RPCFileWriteRequest request,
      final Status status) {
    Preconditions.checkArgument(status != Status.SUCCESS);
    // The response has no payload, so length must be 0.
    return new RPCFileWriteResponse(request.getTempUfsFileId(), request.getOffset(),
        request.getLength(), status);
  }

  @Override
  public Type getType() {
    return Type.RPC_FILE_WRITE_RESPONSE;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCFileWriteResponse} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return the decoded RPCFileWriteResponse object
   */
  public static RPCFileWriteResponse decode(ByteBuf in) {
    long tempUfsFileId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    short status = in.readShort();
    return new RPCFileWriteResponse(tempUfsFileId, offset, length, Status.fromShort(status));
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
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("tempUfsFileId", mTempUfsFileId).add("offset", mOffset)
        .add("length", mLength).add("status", mStatus).toString();
  }

  @Override
  public void validate() {
    Preconditions.checkState(mOffset >= 0, "Offset cannot be negative: %s", mOffset);
    Preconditions.checkState(mLength >= 0 || mLength == -1,
        "Length cannot be negative (except for -1): %s", mLength);
  }

  /**
   * @return the worker specific ufs file id
   */
  public long getTempUfsFileId() {
    return mTempUfsFileId;
  }

  /**
   * @return the number of bytes written
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the file offset that the writing began at
   */
  public long getOffset() {
    return mOffset;
  }

  @Override
  public Status getStatus() {
    return mStatus;
  }
}
