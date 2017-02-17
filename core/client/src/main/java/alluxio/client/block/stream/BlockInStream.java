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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.Locatable;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

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
 *
 * Block lock ownership:
 * The read lock of the block is acquired when the stream is created and released when the
 * stream is closed.
 */
@NotThreadSafe
public final class BlockInStream extends FilterInputStream implements BoundedStream, Seekable,
    PositionedReadable, Locatable {
  private static final long UFS_BLOCK_OPEN_RETRY_INTERVAL_MS = Constants.SECOND_MS;

  /** Helper to manage closeables. */
  private final Closer mCloser;
  private final BlockWorkerClient mBlockWorkerClient;
  private final long mBlockId;
  private final boolean mLocal;
  private final PacketInStream mInputStream;

  /**
   * Creates an instance of local {@link BlockInStream} that reads from local worker.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  public static BlockInStream createLocalBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, InStreamOptions options)
      throws IOException {
    Closer closer = Closer.create();

    BlockWorkerClient blockWorkerClient = null;
    LockBlockResult lockBlockResult = null;
    try {
      blockWorkerClient = closer.register(context.createBlockWorkerClient(workerNetAddress));
      lockBlockResult = blockWorkerClient.lockBlock(blockId, LockBlockOptions.defaults());
      PacketInStream inStream = closer.register(PacketInStream
          .createLocalPacketInstream(lockBlockResult.getBlockPath(), blockId, blockSize));
      return new BlockInStream(inStream, blockId, blockWorkerClient, options);
    } catch (Exception e) {
      if (lockBlockResult != null) {
        blockWorkerClient.unlockBlock(blockId);
      }
      closer.close();
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Creates an instance of remote {@link BlockInStream} that reads from a remote worker.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  public static BlockInStream createRemoteBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, InStreamOptions options)
    throws IOException {
    Closer closer = Closer.create();

    BlockWorkerClient blockWorkerClient = null;
    LockBlockResult lockBlockResult = null;
    try {
      blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      lockBlockResult = blockWorkerClient.lockBlock(blockId, LockBlockOptions.defaults());
      PacketInStream inStream = closer.register(PacketInStream
          .createNettyPacketInStream(context, blockWorkerClient.getDataServerAddress(), blockId,
              lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize,
              Protocol.RequestType.ALLUXIO_BLOCK));
      return new BlockInStream(inStream, blockId, blockWorkerClient, options);
    } catch (Exception e) {
      if (lockBlockResult != null) {
        blockWorkerClient.unlockBlock(blockId);
      }
      closer.close();
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Creates an instance of remote {@link BlockInStream} that reads a block from UFS from a remote
   * worker.
   *
   * @param context the file system context
   * @param ufsPath the UFS path
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockStart the position at which the block starts in the file
   * @param workerNetAddress the worker network address
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Remove create{Local/Remote}BlockInStream and use this one instead.
  public static BlockInStream createUfsBlockInStream(FileSystemContext context, String ufsPath,
      long blockId, long blockSize, long blockStart, boolean isLocal,
      WorkerNetAddress workerNetAddress, InStreamOptions options) throws IOException {
    Closer closer = Closer.create();

    BlockWorkerClient blockWorkerClient = null;
    LockBlockResult lockBlockResult = null;
    try {
      blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockOptions lockBlockOptions =
          LockBlockOptions.defaults().setUfsPath(ufsPath).setOffset(blockStart)
              .setBlockSize(blockSize).setMaxUfsReadConcurrency(options.getMaxUfsReadConcurrency());

      long timeout = System.currentTimeMillis() + Configuration
          .getLong(PropertyKey.USER_UFS_BLOCK_OPEN_TIMEOUT_MS);
      while (true) {
        try {
          lockBlockResult = blockWorkerClient.lockBlock(blockId, lockBlockOptions);
          break;
        } catch (UfsBlockAccessTokenUnavailableException e) {
          if (System.currentTimeMillis() >= timeout) {
            throw e;
          }
          CommonUtils.sleepMs(UFS_BLOCK_OPEN_RETRY_INTERVAL_MS);
        }
      }

      PacketInStream inStream;
      if (lockBlockResult.getLockId() >= 0) {
        // The block is in Alluxio.
        if (isLocal) {
          inStream = closer.register(PacketInStream
              .createLocalPacketInstream(lockBlockResult.getBlockPath(), blockId, blockSize));
        } else {
          inStream = closer.register(PacketInStream
              .createNettyPacketInStream(context, blockWorkerClient.getDataServerAddress(), blockId,
                  lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize,
                  Protocol.RequestType.ALLUXIO_BLOCK));
        }
      } else {
        // The block is not in Alluxio and we have gained access to the UFS.
        inStream = closer.register(PacketInStream
            .createNettyPacketInStream(context, blockWorkerClient.getDataServerAddress(), blockId,
                lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize,
                Protocol.RequestType.UFS_BLOCK));
      }
      return new BlockInStream(inStream, blockId, blockWorkerClient, options);
    } catch (Exception e) {
      if (lockBlockResult != null) {
        blockWorkerClient.unlockBlock(blockId);
      }
      closer.close();
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
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
    return mInputStream.remaining();
  }

  @Override
  public void seek(long pos) throws IOException {
    mInputStream.seek(pos);
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return mInputStream.positionedRead(pos, b, off, len);
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
   * @param inputStream the packet inputstream
   * @param blockId the block ID
   * @param blockWorkerClient the block worker client
   * @param options the options
   * @throws IOException if it fails to create an instance
   */
  private BlockInStream(PacketInStream inputStream, long blockId,
      BlockWorkerClient blockWorkerClient, InStreamOptions options) throws IOException {
    super(inputStream);

    mInputStream = inputStream;
    mBlockId = blockId;
    mBlockWorkerClient = blockWorkerClient;

    mCloser = Closer.create();
    mCloser.register(mInputStream);
    mCloser.register(mBlockWorkerClient);
    try {
      mLocal = blockWorkerClient.getDataServerAddress().getHostName()
          .equals(NetworkAddressUtils.getClientHostName());
      mBlockWorkerClient.accessBlock(blockId);
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }
}
