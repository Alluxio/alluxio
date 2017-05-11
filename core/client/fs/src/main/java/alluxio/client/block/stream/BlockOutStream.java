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
import alluxio.client.QuietlyCancelable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import java.io.FilterOutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling
 * {@link alluxio.client.block.AlluxioBlockStore#getOutStream(long, long, OutStreamOptions)}.
 */
@NotThreadSafe
public class BlockOutStream extends FilterOutputStream implements BoundedStream, QuietlyCancelable {
  private final PacketOutStream mOutStream;
  private boolean mClosed;

  /**
   * Creates a new block output stream that writes to local file directly.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createShortCircuitBlockOutStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, OutStreamOptions options) {
    PacketOutStream outStream = PacketOutStream
        .createLocalPacketOutStream(context, workerNetAddress, blockId, blockSize, options);
    return new BlockOutStream(outStream, options);
  }

  /**
   * Creates a new netty block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createNettyBlockOutStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, OutStreamOptions options) {
    PacketOutStream outStream = PacketOutStream
        .createNettyPacketOutStream(context, workerNetAddress, blockId, blockSize,
            Protocol.RequestType.ALLUXIO_BLOCK, options);
    return new BlockOutStream(outStream, options);
  }

  // Explicitly overriding some write methods which are not efficiently implemented in
  // FilterOutStream.

  @Override
  public void write(byte[] b) {
    mOutStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    mOutStream.write(b, off, len);
  }

  @Override
  public long remaining() {
    return mOutStream.remaining();
  }

  @Override
  public void cancel() {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mOutStream.cancel();
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mOutStream.close();
  }

  /**
   * Creates a new block output stream.
   *
   * @param outStream the {@link PacketOutStream} associated with this {@link BlockOutStream}
   * @param options the options
   */
  protected BlockOutStream(PacketOutStream outStream, OutStreamOptions options) {
    super(outStream);
    mOutStream = outStream;
  }
}
