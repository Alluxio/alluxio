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
import alluxio.client.resource.LockBlockResource;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

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
public class BlockInStream extends FilterInputStream implements BoundedStream, Seekable,
    PositionedReadable, Locatable {
  /** Helper to manage closeables. */
  private final Closer mCloser;
  private final BlockWorkerClient mBlockWorkerClient;
  private final boolean mLocal;
  private final PacketInStream mInputStream;

  /**
   * Creates an instance of {@link BlockInStream} that reads from local file directly.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createShortCircuitBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, InStreamOptions options) {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockResource lockBlockResource =
          closer.register(blockWorkerClient.lockBlock(blockId, LockBlockOptions.defaults()));
      PacketInStream inStream = closer.register(PacketInStream
          .createLocalPacketInStream(lockBlockResource.getResult().getBlockPath(), blockId,
              blockSize, options));
      blockWorkerClient.accessBlock(blockId);
      return new BlockInStream(inStream, blockWorkerClient, closer, options);
    } catch (RuntimeException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
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
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createNettyBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, InStreamOptions options) {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockResource lockBlockResource =
          closer.register(blockWorkerClient.lockBlock(blockId, LockBlockOptions.defaults()));
      PacketInStream inStream = closer.register(PacketInStream
          .createNettyPacketInStream(context, workerNetAddress, blockId,
              lockBlockResource.getResult().getLockId(), blockWorkerClient.getSessionId(),
              blockSize, false, Protocol.RequestType.ALLUXIO_BLOCK, options));
      blockWorkerClient.accessBlock(blockId);
      return new BlockInStream(inStream, blockWorkerClient, closer, options);
    } catch (RuntimeException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * This method keeps polling the block worker until the block is cached to Alluxio or
   * it successfully acquires a UFS read token with a timeout.
   * (1) If the block is cached to Alluxio after polling, it returns {@link BlockInStream}
   *     to read the block from Alluxio storage.
   * (2) If a UFS read token is acquired after polling, it returns {@link BlockInStream}
   *     to read the block from an Alluxio worker that reads the block from UFS.
   * (3) If the polling times out, an {@link IOException} with cause
   *     {@link alluxio.exception.UfsBlockAccessTokenUnavailableException} is thrown.
   *
   * @param context the file system context
   * @param ufsPath the UFS path
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockStart the position at which the block starts in the file
   * @param mountId the id of the UFS which the mount of this file is mapped to
   * @param workerNetAddress the worker network address
   * @param options the options
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createUfsBlockInStream(FileSystemContext context, String ufsPath,
      long blockId, long blockSize, long blockStart, long mountId,
      WorkerNetAddress workerNetAddress, InStreamOptions options) {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockOptions lockBlockOptions =
          LockBlockOptions.defaults().setUfsPath(ufsPath).setOffset(blockStart)
              .setBlockSize(blockSize).setMaxUfsReadConcurrency(options.getMaxUfsReadConcurrency())
              .setMountId(mountId);

      LockBlockResult lockBlockResult =
          closer.register(blockWorkerClient.lockUfsBlock(blockId, lockBlockOptions)).getResult();
      PacketInStream inStream;
      if (lockBlockResult.getLockBlockStatus().blockInAlluxio()) {
        boolean local = workerNetAddress.getHost().equals(NetworkAddressUtils.getClientHostName());
        if (local && Configuration.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED)
            && !NettyUtils.isDomainSocketSupported(workerNetAddress)) {
          inStream = closer.register(PacketInStream
              .createLocalPacketInStream(
                  lockBlockResult.getBlockPath(), blockId, blockSize, options));
        } else {
          inStream = closer.register(PacketInStream
              .createNettyPacketInStream(context, workerNetAddress, blockId,
                  lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize, false,
                  Protocol.RequestType.ALLUXIO_BLOCK, options));
        }
        blockWorkerClient.accessBlock(blockId);
      } else {
        Preconditions.checkState(lockBlockResult.getLockBlockStatus().ufsTokenAcquired());
        inStream = closer.register(PacketInStream
            .createNettyPacketInStream(context, workerNetAddress, blockId,
                lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize,
                !options.getAlluxioStorageType().isStore(), Protocol.RequestType.UFS_BLOCK,
                options));
      }
      return new BlockInStream(inStream, blockWorkerClient, closer, options);
    } catch (RuntimeException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  @Override
  public void close() {
    CommonUtils.close(mCloser);
  }

  @Override
  public long remaining() {
    return mInputStream.remaining();
  }

  @Override
  public void seek(long pos) {
    mInputStream.seek(pos);
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) {
    return mInputStream.positionedRead(pos, b, off, len);
  }

  @Override
  public WorkerNetAddress location() {
    return mBlockWorkerClient.getWorkerNetAddress();
  }

  @Override
  public boolean isLocal() {
    return mLocal;
  }

  /**
   * @return whether this stream is reading directly from a local file
   */
  public boolean isShortCircuit() {
    return mInputStream.isShortCircuit();
  }

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * @param inputStream the packet inputstream
   * @param blockWorkerClient the block worker client
   * @param closer the closer registered with closable resources open so far
   * @param options the options
   */
  protected BlockInStream(PacketInStream inputStream, BlockWorkerClient blockWorkerClient,
      Closer closer, InStreamOptions options) {
    super(inputStream);

    mInputStream = inputStream;
    mBlockWorkerClient = blockWorkerClient;

    mCloser = closer;
    mCloser.register(mInputStream);
    mLocal = blockWorkerClient.getWorkerNetAddress().getHost()
        .equals(NetworkAddressUtils.getClientHostName());
  }
}
