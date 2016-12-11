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
import alluxio.client.Locatable;
import alluxio.client.PositionedReadable;
import alluxio.client.Seekable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.io.Closer;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to read a block from Alluxio. An instance extending this class can be
 * obtained by calling {@link AlluxioBlockStore#getInStream}. Multiple
 * {@link BlockInStream}s can be opened for a block.
 *
 * This class provides the same methods as a Java {@link InputStream} with additional methods from
 * Alluxio Stream interfaces.
 */
@NotThreadSafe
public final class BlockInStream extends FilterInputStream implements BoundedStream, Seekable,
    PositionedReadable, Locatable {
  /** Helper to manage closeables. */
  private final Closer mCloser;
  private final BlockWorkerClient mBlockWorkerClient;
  private final long mBlockId;
  private final boolean mLocal;

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the block store context
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  public static BlockInStream createLocalBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, BlockStoreContext context, InStreamOptions options)
      throws IOException {
    Closer closer = Closer.create();

    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createWorkerClient(workerNetAddress));
      LockBlockResult lockBlockResult = blockWorkerClient.lockBlock(blockId);
      LocalFileBlockReader localFileBlockReader =
          new LocalFileBlockReader(lockBlockResult.getBlockPath());
      PacketReader.Factory factory = new LocalPacketReader.Factory(localFileBlockReader);
      return new BlockInStream(factory, blockId, blockSize, blockWorkerClient, options);
    } catch (AlluxioException e) {
      closer.close();
      throw new IOException(e);
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the block store context
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  public static BlockInStream createRemoteBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, BlockStoreContext context, InStreamOptions options)
    throws IOException {
    Closer closer = Closer.create();

    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createWorkerClient(workerNetAddress));
      LockBlockResult lockBlockResult = blockWorkerClient.lockBlock(blockId);
      PacketReader.Factory factory = new NettyPacketReader.Factory(
          blockWorkerClient.getDataServerAddress(), blockId, lockBlockResult.getLockId(),
          blockWorkerClient.getSessionId());
      return new BlockInStream(factory, blockId, blockSize, blockWorkerClient, options);
    } catch (AlluxioException e) {
      closer.close();
      throw new IOException(e);
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      mBlockWorkerClient.unlockBlock(mBlockId);
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mCloser.close();
    }
  }

  @Override
  public long remaining() {
    return ((PacketInStream) in).remaining();
  }

  @Override
  public void seek(long pos) throws IOException {
    ((PacketInStream) in).seek(pos);
  }

  @Override
  public int read(long pos, byte[] b, int off, int len) throws IOException {
    return ((PacketInStream) in).read(pos, b, off, len);
  }

  @Override
  public InetSocketAddress location() {
    return mBlockWorkerClient.getDataServerAddress();
  }

  @Override
  public boolean isLocal() {
    return mLocal;
  }

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * @param packetReaderFactory the packet reader factory
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockWorkerClient the block worker client
   * @param options the options
   * @throws IOException if it fails to create an instance
   */
  private BlockInStream(PacketReader.Factory packetReaderFactory, long blockId, long blockSize,
      BlockWorkerClient blockWorkerClient, InStreamOptions options) throws IOException {
    super(new PacketInStream(packetReaderFactory, blockId, blockSize));

    mBlockId = blockId;
    mBlockWorkerClient = blockWorkerClient;

    mCloser = Closer.create();
    mCloser.register(in);
    mCloser.register(mBlockWorkerClient);
    try {
      mLocal = blockWorkerClient.getDataServerAddress().getHostName()
          .equals(NetworkAddressUtils.getLocalHostName());
      mBlockWorkerClient.accessBlock(blockId);
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }
}
