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

import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.Locatable;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

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
  private final boolean mLocal;
  private final PacketInStream mInputStream;
  private final WorkerNetAddress mAddress;

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
    PacketInStream inStream = PacketInStream
        .createLocalPacketInStream(context, workerNetAddress, blockId, blockSize, options);
    return new BlockInStream(inStream, workerNetAddress, options);
  }

  /**
   * Creates an instance of remote {@link BlockInStream} that reads from a worker.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param openUfsBlockOptions the open UFS block options
   * @param options the options
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createNettyBlockInStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context,
      Protocol.OpenUfsBlockOptions openUfsBlockOptions, InStreamOptions options) {
    Protocol.ReadRequest.Builder builder = Protocol.ReadRequest.newBuilder().setBlockId(blockId);
    if (openUfsBlockOptions != null) {
      builder.setOpenUfsBlockOptions(openUfsBlockOptions);
    }

    PacketInStream inStream = PacketInStream
        .createNettyPacketInStream(context, workerNetAddress, builder.buildPartial(), blockSize,
            options);
    return new BlockInStream(inStream, workerNetAddress, options);
  }

  @Override
  public void close() {
    try {
      mInputStream.close();
    } catch (IOException e) {
      throw AlluxioStatusException.fromIOException(e);
    }
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
    return mAddress;
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
   * @param workerNetAddress the worker network address
   * @param options the options
   */
  protected BlockInStream(PacketInStream inputStream, WorkerNetAddress workerNetAddress,
      InStreamOptions options) {
    super(inputStream);
    mInputStream = inputStream;
    mAddress = workerNetAddress;
    mLocal = workerNetAddress.getHost().equals(NetworkAddressUtils.getClientHostName());
  }
}
