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
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The internal representation of a block read request.
 */
@ThreadSafe
public final class BlockReadRequest extends AbstractReadHandler.ReadRequest {
  private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final boolean mPromote;
  private final Context mContext = new Context();

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param request the block read request
   */
  BlockReadRequest(Protocol.ReadRequest request) throws Exception {
    super(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
        request.getPacketSize());

    if (request.hasOpenUfsBlockOptions()) {
      mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
    } else {
      mOpenUfsBlockOptions = null;
    }
    mPromote = request.getPromote();
    // Note that we do not need to seek to offset since the block worker is created at the offset.
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

  /**
   * @return the context of this request
   */
  public Context getContext() {
    return mContext;
  }

  /**
   * The context of this request, including some runtime state to handle this request.
   */
  @NotThreadSafe
  final class Context {
    private BlockReader mBlockReader;
    private Counter mCounter;

    public Context() {}

    /**
     * @return block reader
     */
    public BlockReader getBlockReader() {
      Preconditions.checkState(mBlockReader != null);
      return mBlockReader;
    }

    /**
     * @return counter
     */
    public Counter getCounter() {
      Preconditions.checkState(mCounter != null);
      return mCounter;
    }

    /**
     * Sets the block reader.
     *
     * @param blockReader block reader to set
     */
    public void setBlockReader(BlockReader blockReader) {
      mBlockReader = blockReader;
    }

    /**
     * Sets the counter.
     *
     * @param counter counter to set
     */
    public void setCounter(Counter counter) {
      mCounter = counter;
    }
  }
}
