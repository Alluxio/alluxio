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
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * The block write request internal representation. When this request is closed, it will commit the
 * block.
 */
final class BlockWriteRequest extends AbstractWriteRequest {
  private BlockWriter mBlockWriter;
  private long mBytesReserved;
  private final BlockWriteHandler mBlockWriteHandler;
  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;

  BlockWriteRequest(BlockWriteHandler blockWriteHandler, Protocol.WriteRequest request,
      long bytesReserved, BlockWorker worker) throws Exception {
    super(request.getId());
    Preconditions.checkState(request.getOffset() == 0);
    mBlockWriteHandler = blockWriteHandler;
    mWorker = worker;
    mWorker.createBlockRemote(getSessionId(), getId(),
        mBlockWriteHandler.getStorageTierAssoc().getAlias(request.getTier()), bytesReserved);
    mBytesReserved = bytesReserved;
  }

  @Override
  public void close(Channel channel) throws IOException {
    if (mBlockWriter != null) {
      mBlockWriter.close();
    }
    try {
      mWorker.commitBlock(getSessionId(), getId());
    } catch (Exception e) {
      throw CommonUtils.castToIOException(e);
    }
  }

  @Override
  void cancel() throws IOException {
    if (mBlockWriter != null) {
      mBlockWriter.close();
    }
    try {
      mWorker.abortBlock(getSessionId(), getId());
    } catch (Exception e) {
      throw CommonUtils.castToIOException(e);
    }
  }

  @Override
  void cleanup() throws IOException {
    mWorker.cleanupSession(getSessionId());
  }

  /**
   * @return the block writer
   */
  @Nullable
  public BlockWriter getBlockWriter() {
    return mBlockWriter;
  }

  /**
   * @return the bytes reserved
   */
  public long getBytesReserved() {
    return mBytesReserved;
  }

  /**
   * Sets the block writer.
   *
   * @param blockWriter block writer to set
   */
  public void setBlockWriter(BlockWriter blockWriter) {
    mBlockWriter = blockWriter;
  }

  /**
   * Sets the bytes reserved.
   *
   * @param bytesReserved the bytes reserved to set
   */
  public void setBytesReserved(long bytesReserved) {
    mBytesReserved = bytesReserved;
  }
}
