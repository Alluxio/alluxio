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

package alluxio.wire;

import alluxio.proto.dataserver.Protocol;
import alluxio.util.IdUtils;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The internal representation of a block read request.
 */
@ThreadSafe
public final class BlockReadRequest {
  private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final long mId;
  private final long mStart;
  private final long mEnd;
  private final long mChunkSize;
  private final long mSessionId;
  private final boolean mPromote;
  private final boolean mPositionShort;

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param request the block read request
   */
  public BlockReadRequest(alluxio.grpc.ReadRequest request) {
    this(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
        request.getChunkSize(), request.getPromote(), request.getPositionShort(),
        request.hasOpenUfsBlockOptions() ? request.getOpenUfsBlockOptions() : null);
  }

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param id block id
   * @param start start offset of this request
   * @param end start offset of this request
   * @param chunkSize chunk size in bytes
   * @param promote whether to promote the block to top tier
   * @param positionShort whether this is a short read
   * @param openUfsBlockOptions options to read file from UFS
   */
  public BlockReadRequest(long id, long start, long end, long chunkSize, boolean promote,
      boolean positionShort, Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    mId = id;
    mStart = start;
    mEnd = end;
    mChunkSize = chunkSize;
    mSessionId = IdUtils.createSessionId();
    mOpenUfsBlockOptions = openUfsBlockOptions;
    mPromote = promote;
    mPositionShort = positionShort;
  }

  /**
   * @return session Id
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return block id of the read request
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the start offset in bytes of this read request
   */
  public long getStart() {
    return mStart;
  }

  /**
   * @return the end offset in bytes of this read request
   */
  public long getEnd() {
    return mEnd;
  }

  /**
   * @return the chunk size in bytes of this read request
   */
  public long getChunkSize() {
    return mChunkSize;
  }

  /**
   * @return if the block read type indicate promote in tier storage
   */
  public boolean isPromote() {
    return mPromote;
  }

  /**
   * @return if this is a positioned read to a small buffer
   */
  public boolean isPositionShort() {
    return mPositionShort;
  }

  /**
   * @return the option to open UFS block
   */
  public Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions() {
    return mOpenUfsBlockOptions;
  }

  /**
   * @return true if the block is persisted in UFS
   */
  public boolean isPersisted() {
    return mOpenUfsBlockOptions != null && mOpenUfsBlockOptions.hasUfsPath();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("chunkSize", getChunkSize())
        .add("end", getEnd())
        .add("id", getId())
        .add("openUfsBlockOptions", mOpenUfsBlockOptions)
        .add("promote", mPromote)
        .add("sessionId", getSessionId())
        .add("start", getStart())
        .add("positionShort", isPositionShort())
        .toString();
  }
}
