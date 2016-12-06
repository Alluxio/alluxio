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

package alluxio.client.block.stream;

import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import com.google.common.io.Closer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class BlockInStream extends PacketInStream {
  /** Helper to manage closeables. */
  protected final Closer mCloser;
  protected final BlockWorkerClient mBlockWorkerClient;
  protected final LockBlockResult mLockBlockResult;

  public BlockInStream(long blockId, long blockSize, WorkerNetAddress workerNetAddress,
      BlockStoreContext context, InStreamOptions options) throws IOException {
    super(blockId, blockSize);

    mCloser = Closer.create();
    try {
      mBlockWorkerClient = mCloser.register(context.createWorkerClient(workerNetAddress));
      mLockBlockResult = mBlockWorkerClient.lockBlock(blockId);
      if (mLockBlockResult == null) {
        throw new IOException(ExceptionMessage.BLOCK_NOT_LOCALLY_AVAILABLE.getMessage(blockId));
      }
      mBlockWorkerClient.accessBlock(mId);
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      closePacketReader();
      mBlockWorkerClient.unlockBlock(mId);
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }
}
