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

package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The internal representation of a block read request.
 */
@ThreadSafe
// TODO(jiacheng): This is not removed because it is used by the FileReadHandler.
//  It shall be removed together with a refactor on the Dora worker refactor.
public final class BlockReadRequest extends ReadRequest {
  private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final boolean mPromote;

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param request the block read request
   */
  public BlockReadRequest(Protocol.ReadRequest request) {
    super(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
        downcastChunkSize(request));

    if (request.hasOpenUfsBlockOptions()) {
      mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
    } else {
      mOpenUfsBlockOptions = null;
    }
    mPromote = request.getPromote();
    // Note that we do not need to seek to offset since the block worker is created at the offset.
  }

  private static int downcastChunkSize(Protocol.ReadRequest request) {
    int chunkSizeInt = (int) request.getChunkSize();
    Preconditions.checkArgument(request.getChunkSize() == (long) chunkSizeInt,
        "chunk size does not fit in an int: %s", request.getChunkSize());
    return chunkSizeInt;
  }

  /**
   * @return if the block read type indicate promote in tier storage
   */
  public boolean isPromote() {
    return mPromote;
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
}
