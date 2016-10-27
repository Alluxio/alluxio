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

import alluxio.Constants;
import alluxio.client.RemoteBlockReader;
import alluxio.client.block.BlockStoreContext;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

import com.codahale.metrics.Counter;
import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Read data from remote data server using Netty.
 */
@NotThreadSafe
public final class NettyRemoteBlockReader implements RemoteBlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Callable<Bootstrap> mClientBootstrap;
  /** A reference to read response so we can explicitly release the resource after reading. */
  private RPCBlockReadResponse mReadResponse = null;

  /**
   * Creates a new {@link NettyRemoteBlockReader}.
   */
  public NettyRemoteBlockReader() {
    mClientBootstrap = NettyClient.bootstrapBuilder();
  }

  /**
   * Constructor for unittest only.
   *
   * @param clientBootstrap bootstrap class of the client channel
   */
  public NettyRemoteBlockReader(final Bootstrap clientBootstrap) {
    mClientBootstrap = new Callable<Bootstrap>() {
      @Override
      public Bootstrap call() {
        return clientBootstrap;
      }
    };
  }

  @Override
  public ByteBuffer readRemoteBlock(InetSocketAddress address, long blockId, long offset,
      long length, long lockId, long sessionId) throws IOException {
    SingleResponseListener listener = null;
    Channel channel = null;
    Metrics.NETTY_BLOCK_READ_OPS.inc();
    try {
      channel = BlockStoreContext.acquireNettyChannel(address, mClientBootstrap);
      listener = new SingleResponseListener();
      channel.pipeline().get(ClientHandler.class).addListener(listener);
      ChannelFuture channelFuture = channel
          .writeAndFlush(new RPCBlockReadRequest(blockId, offset, length, lockId, sessionId));
      channelFuture = channelFuture.sync();
      if (channelFuture.isDone() && !channelFuture.isSuccess()) {
        LOG.error("Failed to write to %s for block %d with error %s.", address.toString(), blockId,
            channelFuture.cause());
        throw new IOException(channelFuture.cause());
      }

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);

      switch (response.getType()) {
        case RPC_BLOCK_READ_RESPONSE:
          RPCBlockReadResponse blockResponse = (RPCBlockReadResponse) response;
          LOG.debug("Data {} from remote machine {} received", blockId, address);

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
      Metrics.NETTY_BLOCK_READ_FAILURES.inc();
      try {
        if (channel != null) {
          channel.close().sync();
        }
      } catch (InterruptedException ee) {
        throw Throwables.propagate(ee);
      }
      throw new IOException(e);
    } finally {
      if (channel != null && listener != null && channel.isActive()) {
        channel.pipeline().get(ClientHandler.class).removeListener(listener);
      }
      if (channel != null) {
        BlockStoreContext.releaseNettyChannel(address, channel);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Release the underlying buffer of previous/current read response.
   */
  @Override
  public void close() throws IOException {
    if (mReadResponse != null) {
      mReadResponse.getPayloadDataBuffer().release();
      mReadResponse = null;
    }
  }

  /**
   * Class that contains metrics about {@link NettyRemoteBlockReader}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter NETTY_BLOCK_READ_OPS =
        MetricsSystem.clientCounter("NettyBlockReadOps");
    private static final Counter NETTY_BLOCK_READ_FAILURES =
        MetricsSystem.clientCounter("NettyBlockReadFailures");

    private Metrics() {} // prevent instantiation
  }
}
