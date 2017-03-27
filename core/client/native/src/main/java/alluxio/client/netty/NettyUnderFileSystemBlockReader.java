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

package alluxio.client.netty;

import alluxio.client.UnderFileSystemBlockReader;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.RPCUnderFileSystemBlockReadRequest;

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Read data from UFS on a data server using Netty.
 */
@NotThreadSafe
public final class NettyUnderFileSystemBlockReader implements UnderFileSystemBlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(NettyUnderFileSystemBlockReader.class);

  private final FileSystemContext mContext;
  /** A reference to read response so we can explicitly release the resource after reading. */
  private RPCBlockReadResponse mReadResponse = null;

  /**
   * Creates a new {@link NettyUnderFileSystemBlockReader}.
   * @param context the file system context
   */
  public NettyUnderFileSystemBlockReader(FileSystemContext context) {
    mContext = context;
  }

  @Override
  public ByteBuffer read(InetSocketAddress address, long blockId, long offset,
      long length, long sessionId, boolean noCache) throws IOException {
    Channel channel = null;
    ClientHandler clientHandler = null;
    Metrics.NETTY_UFS_BLOCK_READ_OPS.inc();
    try {
      channel = mContext.acquireNettyChannel(address);
      if (!(channel.pipeline().last() instanceof ClientHandler)) {
        channel.pipeline().addLast(new ClientHandler());
      }
      clientHandler = (ClientHandler) channel.pipeline().last();
      SingleResponseListener listener = new SingleResponseListener();
      clientHandler.addListener(listener);

      ChannelFuture channelFuture = channel.writeAndFlush(
          new RPCUnderFileSystemBlockReadRequest(blockId, offset, length, sessionId, noCache));
      channelFuture = channelFuture.sync();
      if (channelFuture.isDone() && !channelFuture.isSuccess()) {
        LOG.error("Failed to read from %s for block %d with error %s.", address.toString(), blockId,
            channelFuture.cause());
        throw new IOException(channelFuture.cause());
      }

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);

      switch (response.getType()) {
        case RPC_BLOCK_READ_RESPONSE:
          RPCBlockReadResponse blockResponse = (RPCBlockReadResponse) response;
          LOG.debug("Data {} from machine {} received", blockId, address);

          RPCResponse.Status status = blockResponse.getStatus();
          if (status == RPCResponse.Status.SUCCESS) {
            // always clear the previous response before reading another one
            close();
            mReadResponse = blockResponse;
            return blockResponse.getPayloadDataBuffer().getReadOnlyByteBuffer();
          }
          throw new IOException(status.getMessage() + " response: " + blockResponse);
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
              .getMessage(response.getType(), RPCMessage.Type.RPC_BLOCK_READ_RESPONSE));
      }
    } catch (Exception e) {
      Metrics.NETTY_UFS_BLOCK_READ_FAILURES.inc();
      try {
        if (channel != null) {
          channel.close().sync();
        }
      } catch (InterruptedException ee) {
        throw new RuntimeException(ee);
      }
      throw new IOException(e);
    } finally {
      if (clientHandler != null) {
        clientHandler.removeListeners();
      }
      if (channel != null) {
        mContext.releaseNettyChannel(address, channel);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Releases the underlying buffer of previous/current read response.
   */
  @Override
  public void close() throws IOException {
    if (mReadResponse != null) {
      mReadResponse.getPayloadDataBuffer().release();
      mReadResponse = null;
    }
  }

  /**
   * Class that contains metrics about {@link NettyUnderFileSystemBlockReader}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter NETTY_UFS_BLOCK_READ_OPS =
        MetricsSystem.clientCounter("NettyUFSBlockReadOps");
    private static final Counter NETTY_UFS_BLOCK_READ_FAILURES =
        MetricsSystem.clientCounter("NettyUFSBlockReadFailures");

    private Metrics() {} // prevent instantiation
  }
}
