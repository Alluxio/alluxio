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
import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.wire.WorkerNetAddress;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling
 * {@link alluxio.client.block.AlluxioBlockStore#getOutStream(long, long, OutStreamOptions)}.
 */
@NotThreadSafe
public class BlockOutStream extends FilterOutputStream implements BoundedStream, Cancelable {
  private final PacketOutStream mOutStream;
  private boolean mClosed;

  /**
   * Creates an {@link BlockOutStream}.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the out stream options
   * @return the {@link OutputStream} object
   */
  public static BlockOutStream create(FileSystemContext context, long blockId, long blockSize,
      WorkerNetAddress address, OutStreamOptions options) throws IOException {
    if (CommonUtils.isLocalHost(address) && Configuration
        .getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED) && !NettyUtils
        .isDomainSocketSupported(address)) {
      PacketOutStream outStream = PacketOutStream
          .createLocalPacketOutStream(context, address, blockId, blockSize, options);
      return new BlockOutStream(outStream);
    } else {
      Protocol.WriteRequest writeRequestPartial = Protocol.WriteRequest.newBuilder()
          .setId(blockId).setTier(options.getWriteTier()).setType(Protocol.RequestType.ALLUXIO_BLOCK)
          .buildPartial();
      PacketOutStream outStream = PacketOutStream
          .createNettyPacketOutStream(context, address, blockSize, writeRequestPartial, options);
      return new BlockOutStream(outStream);
    }
  }

  // Explicitly overriding some write methods which are not efficiently implemented in
  // FilterOutStream.

  @Override
  public void write(byte[] b) throws IOException {
    mOutStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOutStream.write(b, off, len);
  }

  @Override
  public long remaining() {
    return mOutStream.remaining();
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mOutStream.cancel();
  }

  @Override
  public void close() throws IOException {
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
   */
  protected BlockOutStream(PacketOutStream outStream) {
    super(outStream);
    mOutStream = outStream;
    mClosed = false;
  }
}
