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

import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import com.google.common.io.Closer;

import java.io.FilterOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling
 * {@link alluxio.client.block.AlluxioBlockStore#getOutStream(long, long, OutStreamOptions)}.
 */
@NotThreadSafe
public final class BlockOutStream extends FilterOutputStream implements BoundedStream, Cancelable {
  private final long mBlockId;
  private final long mBlockSize;
  private final Closer mCloser;
  private final BlockWorkerClient mBlockWorkerClient;

  /**
   * Creates a new local block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the block store context
   * @param options the options
   * @throws IOException if an I/O error occurs
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createLocalBlockOutStream(long blockId,
      long blockSize,
      WorkerNetAddress workerNetAddress,
      BlockStoreContext context,
      OutStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient client = closer.register(context.createWorkerClient(workerNetAddress));
      PacketWriter packetWriter =
          closer.register(LocalPacketWriter.createLocalPacketWriter(client, blockId));
      return new BlockOutStream(blockId, blockSize, client, packetWriter, options);
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

  /**
   * Creates a new remote block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the block store context
   * @param options the options
   * @throws IOException if an I/O error occurs
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createRemoteBlockOutStream(long blockId,
      long blockSize,
      WorkerNetAddress workerNetAddress,
      BlockStoreContext context,
      OutStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient client = closer.register(context.createWorkerClient(workerNetAddress));
      PacketWriter packetWriter = closer.register(
          new NettyPacketWriter(client.getDataServerAddress(), blockId, client.getSessionId(),
              Protocol.RequestType.ALLUXIO_BLOCK));
      return new BlockOutStream(blockId, blockSize, client, packetWriter, options);
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

  @Override
  public long remaining() {
    return ((PacketOutStream) out).remaining();
  }

  @Override
  public void cancel() throws IOException {
    try {
      ((PacketOutStream) out).cancel();
      mBlockWorkerClient.cancelBlock(mBlockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (remaining() < mBlockSize) {
        mBlockWorkerClient.cacheBlock(mBlockId);
      }
    } catch (AlluxioException e) {
      mCloser.rethrow(new IOException(e));
    } catch (Throwable e) {
      mCloser.rethrow(e);
    } finally {
      mCloser.close();
    }
  }

  /**
   * Creates a new block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param options the options
   * @throws IOException if an I/O error occurs
   */
  private BlockOutStream(long blockId,
      long blockSize,
      BlockWorkerClient blockWorkerClient,
      PacketWriter packetWriter,
      OutStreamOptions options) throws IOException {
    super(new PacketOutStream(packetWriter, blockSize));

    mBlockId = blockId;
    mBlockSize = blockSize;
    mCloser = Closer.create();
    mCloser.register(out);
    mBlockWorkerClient = mCloser.register(blockWorkerClient);
  }
}
