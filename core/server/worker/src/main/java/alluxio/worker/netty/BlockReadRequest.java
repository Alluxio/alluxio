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
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The block read request internal representation. When this request is closed, it will clean
 * up any temporary state it may have accumulated.
 */
final class BlockReadRequest extends AbstractReadRequest {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadRequest.class);

  private BlockReader mBlockReader;
  private Counter mCounter;
  private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final boolean mPromote;
  /** The Block Worker. */
  private final BlockWorker mWorker;

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param request the block read request
   */
  BlockReadRequest(Protocol.ReadRequest request, BlockWorker worker) throws Exception {
    super(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
        request.getPacketSize());

    if (request.hasOpenUfsBlockOptions()) {
      mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
    } else {
      mOpenUfsBlockOptions = null;
    }
    mPromote = request.getPromote();
    mWorker = worker;
    // Note that we do not need to seek to offset since the block worker is created at the offset.
  }

  /**
   * @return block reader
   */
  public BlockReader getBlockReader() {
    return mBlockReader;
  }

  /**
   * @return counter
   */
  public Counter getCounter() {
    return mCounter;
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
  boolean isPersisted() {
    return mOpenUfsBlockOptions != null && mOpenUfsBlockOptions.hasUfsPath();
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

  @Override
  public void close() {
    if (mBlockReader != null) {
      try {
        mBlockReader.close();
      } catch (Exception e) {
        LOG.warn(
            "Failed to close block reader for block {} with error {}.", getId(), e.getMessage());
      }
    }

    try {
      if (!mWorker.unlockBlock(getSessionId(), getId())) {
        mWorker.closeUfsBlock(getSessionId(), getId());
      }
    } catch (Exception e) {
      LOG.warn("Failed to unlock block {} with error {}.", getId(), e.getMessage());
    }
  }
}
