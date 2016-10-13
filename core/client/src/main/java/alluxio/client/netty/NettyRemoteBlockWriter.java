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
import alluxio.client.RemoteBlockWriter;
import alluxio.client.block.BlockStoreContext;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCBlockWriteResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;

import com.codahale.metrics.Counter;
import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Write data to a remote data server using Netty.
 */
@NotThreadSafe
public final class NettyRemoteBlockWriter implements RemoteBlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Callable<Bootstrap> mClientBootstrap;

  private boolean mOpen;
  private InetSocketAddress mAddress;
  private long mBlockId;
  private long mSessionId;

  // Total number of bytes written to the remote block.
  private long mWrittenBytes;

  /**
   * Creates a new {@link NettyRemoteBlockWriter}.
   */
  public NettyRemoteBlockWriter() {
    mClientBootstrap = NettyClient.bootstrapBuilder();
    mOpen = false;
  }

  @Override
  public void open(InetSocketAddress address, long blockId, long sessionId) throws IOException {
    if (mOpen) {
      throw new IOException(
          ExceptionMessage.WRITER_ALREADY_OPEN.getMessage(mAddress, mBlockId, mSessionId));
    }
    mAddress = address;
    mBlockId = blockId;
    mSessionId = sessionId;
    mWrittenBytes = 0;
    mOpen = true;
  }

  @Override
  public void close() {
    if (mOpen) {
      mOpen = false;
    }
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    SingleResponseListener listener = null;
    Channel channel = null;
    Metrics.NETTY_BLOCK_WRITE_OPS.inc();
    try {
      channel = BlockStoreContext.acquireNettyChannel(mAddress, mClientBootstrap);
      listener = new SingleResponseListener();
      channel.pipeline().get(ClientHandler.class).addListener(listener);
      ChannelFuture channelFuture = channel.writeAndFlush(
          new RPCBlockWriteRequest(mSessionId, mBlockId, mWrittenBytes, length,
              new DataByteArrayChannel(bytes, offset, length))).sync();
      if (channelFuture.isDone() && !channelFuture.isSuccess()) {
        LOG.error("Failed to write to %s for block %d with error %s.", mAddress.toString(),
            mBlockId, channelFuture.cause());
        throw new IOException(channelFuture.cause());
      }

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);

      switch (response.getType()) {
        case RPC_BLOCK_WRITE_RESPONSE:
          RPCBlockWriteResponse resp = (RPCBlockWriteResponse) response;
          RPCResponse.Status status = resp.getStatus();
          LOG.debug("status: {} from remote machine {} received", status, mAddress);

          if (status != RPCResponse.Status.SUCCESS) {
            throw new IOException(ExceptionMessage.BLOCK_WRITE_ERROR.getMessage(mBlockId,
                mSessionId, mAddress, status.getMessage()));
          }
          mWrittenBytes += length;
          break;
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
              .getMessage(response.getType(), RPCMessage.Type.RPC_BLOCK_WRITE_RESPONSE));
      }
    } catch (Exception e) {
      Metrics.NETTY_BLOCK_WRITE_FAILURES.inc();
      try {
        // TODO(peis): We should not close the channel unless it is an exception caused by network.
        if (channel != null) {
          channel.close().sync();
        }
      } catch (InterruptedException ee) {
        Throwables.propagate(ee);
      }
      throw new IOException(e);
    } finally {
      if (channel != null && listener != null && channel.isActive()) {
        channel.pipeline().get(ClientHandler.class).removeListener(listener);
      }
      if (channel != null) {
        BlockStoreContext.releaseNettyChannel(mAddress, channel);
      }
    }
  }

  /**
   * Class that contains metrics about {@link NettyRemoteBlockWriter}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter NETTY_BLOCK_WRITE_OPS =
        MetricsSystem.clientCounter("NettyBlockWriteOps");
    private static final Counter NETTY_BLOCK_WRITE_FAILURES =
        MetricsSystem.clientCounter("NettyBlockWriteFailures");

    private Metrics() {} // prevent instantiation
  }
}
