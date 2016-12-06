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
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling {@link AlluxioBlockStore#getOutStream}.
 *
 * <p>
 * The type of {@link PacketOutStream} returned will depend on the user configuration and
 * cluster setup. A {@link LocalPacketOutStream} is returned if the client is co-located with an
 * Alluxio worker and the user has enabled this optimization. Otherwise,
 * {@link RemotePacketOutStream} will be returned which will write the data through an Alluxio
 * worker.
 */
@NotThreadSafe
public abstract class BlockOutStream extends PacketOutStream {
  protected final BlockWorkerClient mBlockWorkerClient;

  /**
   * Constructs a new {@link PacketOutStream}.
   *
   * @param blockId the id of the block
   * @param blockSize the size of the block
   */
  public BlockOutStream(WorkerNetAddress workerNetAddress, long blockId, long blockSize,
      BlockStoreContext context, OutStreamOptions options) throws IOException {
    super(blockId, blockSize);

    try {
      mBlockWorkerClient = mCloser.register(context.createWorkerClient(workerNetAddress));
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      updateCurrentPacket(true);
      mBlockWorkerClient.cancelBlock(mId);
      mPacketWriter.cancel();
    } catch (AlluxioException e) {
      throw mCloser.rethrow(new IOException(e));
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      updateCurrentPacket(true);
      mPacketWriter.close();
      if (mPacketWriter.pos() > 0) {
        mBlockWorkerClient.cacheBlock(mId);
      }
    } catch (AlluxioException e) {
      throw mCloser.rethrow(new IOException(e));
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }
}
