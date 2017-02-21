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
import alluxio.util.network.NetworkAddressUtils;
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
          .createRemoteBlockOutStream(blockId, blockSize, address, context, options);
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
      return BlockInStream.createRemoteBlockInStream(blockId, blockSize, address, context, options);
    } else {
      return new alluxio.client.block.RemoteBlockInStream(blockId, blockSize, address, context,
          options);
    }
  }

  /**
   * Creates an {@link InputStream} for a UFS block.
   *
   * @param context the file system context
   * @param ufsPath the UFS path
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockStart the start position of the block in the UFS file
   * @param address the worker network address
   * @param options the in stream options
   * @return the input stream
   * @throws IOException if it fails to create the input stream
   */
  public static InputStream createUfsBlockInStream(FileSystemContext context, String ufsPath,
      long blockId, long blockSize, long blockStart, WorkerNetAddress address,
      InStreamOptions options) throws IOException {
    InputStream inputStream;
    if (PACKET_STREAMING_ENABLED) {
      inputStream = BlockInStream
          .createUfsBlockInStream(context, ufsPath, blockId, blockSize, blockStart, address,
              options);
    } else {
      inputStream = UfsBlockInStream
          .createUfsBlockInStream(context, ufsPath, blockId, blockSize, blockStart, address,
              options);
    }

    if (inputStream == null) {
      if (address.getHost().equals(NetworkAddressUtils.getLocalHostName())) {
        inputStream = createLocalBlockInStream(context, blockId, blockSize, address, options);
      } else {
        inputStream = createRemoteBlockInStream(context, blockId, blockSize, address, options);
      }
    }
    return inputStream;
  }
}
