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

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The internal representation of a block read request.
 */
@ThreadSafe
public final class BlockReadRequest extends ReadRequest {
  private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final boolean mPromote;
  private final boolean mPositionShort;

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param request the block read request
   */
  public BlockReadRequest(alluxio.grpc.ReadRequest request) {
    super(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
        request.getChunkSize());

    if (request.hasOpenUfsBlockOptions()) {
      mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
    } else {
      mOpenUfsBlockOptions = null;
    }
    mPromote = request.getPromote();
    mPositionShort = request.getPositionShort();
    // Note that we do not need to seek to offset since the block worker is created at the offset.
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
