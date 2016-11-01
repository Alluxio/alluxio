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
import alluxio.client.block.stream.NettyBlockReader;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handles all the messages received by the client channel.
 */
@NotThreadSafe
public final class BlockReadClientHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private NettyBlockReader.PacketQueue mPacketQueue;

  /**
   * Creates a new {@link BlockReadClientHandler}.
   */
  public BlockReadClientHandler() {
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (!(msg instanceof RPCMessage)) {
        mPacketQueue
            .exceptionCaught(new IOException("Corrupted data received while reading file."));
        return;
      }

      RPCMessage response = (RPCMessage) msg;
      switch (response.getType()) {
        case RPC_BLOCK_READ_RESPONSE:
          RPCBlockReadResponse blockResponse = (RPCBlockReadResponse) response;
          RPCResponse.Status status = blockResponse.getStatus();
          LOG.debug("Data {} received", blockResponse.getBlockId());
          if (status == RPCResponse.Status.SUCCESS || status == RPCResponse.Status.STREAM_PACKET) {
            mPacketQueue.offerPacket((ByteBuf) blockResponse.getPayloadDataBuffer().getNettyOutput(),
                status == RPCResponse.Status.SUCCESS);
          } else {
            mPacketQueue.exceptionCaught(new IOException(blockResponse.toString()));
          }
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          mPacketQueue.exceptionCaught(new IOException(error.getStatus().getMessage()));
      }
   } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    mPacketQueue.exceptionCaught(cause);
  }
}
