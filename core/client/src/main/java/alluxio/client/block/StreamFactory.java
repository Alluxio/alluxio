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

package alluxio.client.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.netty.NettyClient;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A factory class to create various block streams in Alluxio.
 */
@NotThreadSafe
public final class StreamFactory {
  private static final boolean PACKET_STREAMING_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_PACKET_STREAMING_ENABLED);

  private StreamFactory() {} // prevent instantiation

  /**
   * Creates an {@link OutputStream} that writes to a block on local worker.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the out stream options
   * @return the {@link OutputStream} object
   * @throws IOException if it fails to create the output stream
   */
  public static OutputStream createLocalBlockOutStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, OutStreamOptions options) throws IOException {
    if (PACKET_STREAMING_ENABLED) {
      if (!address.getDomainSocketPath().isEmpty() && NettyClient.isDomainSocketEnabled()) {
        return BlockOutStream
            .createNettyBlockOutStream(blockId, blockSize, address, context, true, options);
      }
      return BlockOutStream
          .createLocalBlockOutStream(blockId, blockSize, address, context, options);
    } else {
      return new alluxio.client.block.LocalBlockOutStream(blockId, blockSize, address, context,
          options);
    }
  }

  /**
   * Creates an {@link OutputStream} that writes to a remote worker.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the out stream options
   * @return the {@link OutputStream} object
   * @throws IOException if it fails to create the output stream
   */
  public static OutputStream createRemoteBlockOutStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, OutStreamOptions options) throws IOException {
    if (PACKET_STREAMING_ENABLED) {
      return BlockOutStream
          .createNettyBlockOutStream(blockId, blockSize, address, context, false, options);
    } else {
      return new alluxio.client.block.RemoteBlockOutStream(blockId, blockSize, address, context,
          options);
    }
  }

  /**
   * Creates an {@link InputStream} that writes to a local block.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the in stream options
   * @return the {@link InputStream} object
   * @throws IOException if it fails to create the input stream
   */
  public static InputStream createLocalBlockInStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, InStreamOptions options) throws IOException {
    if (PACKET_STREAMING_ENABLED) {
      if (!address.getDomainSocketPath().isEmpty() && NettyClient.isDomainSocketEnabled()) {
        return BlockInStream
            .createNettyBlockInStream(blockId, blockSize, address, context, true, options);
      }
      return BlockInStream.createLocalBlockInStream(blockId, blockSize, address, context, options);
    } else {
      return new alluxio.client.block.LocalBlockInStream(blockId, blockSize, address, context,
          options);
    }
  }

  /**
   * Creates an {@link InputStream} that writes to a local block.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the in stream options
   * @return the {@link InputStream} object
   * @throws IOException if it fails to create the input stream
   */
  public static InputStream createRemoteBlockInStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, InStreamOptions options) throws IOException {
    if (PACKET_STREAMING_ENABLED) {
      return BlockInStream
          .createNettyBlockInStream(blockId, blockSize, address, context, false, options);
    } else {
      return new alluxio.client.block.RemoteBlockInStream(blockId, blockSize, address, context,
          options);
    }
  }
}
