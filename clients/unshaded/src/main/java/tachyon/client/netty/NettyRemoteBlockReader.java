/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import tachyon.Constants;
import tachyon.client.RemoteBlockReader;
import tachyon.conf.TachyonConf;
import tachyon.network.ChannelType;
import tachyon.network.NettyUtils;
import tachyon.network.protocol.RPCBlockRequest;
import tachyon.network.protocol.RPCBlockResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCMessageDecoder;
import tachyon.network.protocol.RPCMessageEncoder;
import tachyon.network.protocol.RPCResponse;

/**
 * Read data from remote data server using Netty.
 */
public final class NettyRemoteBlockReader implements RemoteBlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Bootstrap mClientBootstrap;
  private final ClientHandler mHandler;

  // TODO: Creating a new remote block reader may be expensive, so consider a connection pool.
  public NettyRemoteBlockReader() {
    mHandler = new ClientHandler();
    mClientBootstrap = NettyClient.createClientBootstrap(mHandler);
  }

  @Override
  public ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException {
    InetSocketAddress address = new InetSocketAddress(host, port);

    try {
      ChannelFuture f = mClientBootstrap.connect(address).sync();

      LOG.info("Connected to remote machine " + address);
      Channel channel = f.channel();
      SingleResponseListener listener = new SingleResponseListener();
      mHandler.addListener(listener);
      channel.writeAndFlush(new RPCBlockRequest(blockId, offset, length));

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
      channel.close().sync();

      if (response.getType() == RPCMessage.Type.RPC_BLOCK_RESPONSE) {
        RPCBlockResponse blockResponse = (RPCBlockResponse) response;
        LOG.info("Data " + blockId + " from remote machine " + address + " received");

        if (blockResponse.getBlockId() < 0) {
          LOG.info("Data " + blockResponse.getBlockId() + " is not in remote machine.");
          return null;
        }
        return blockResponse.getPayloadDataBuffer().getReadOnlyByteBuffer();
      } else {
        LOG.error("Unexpected response message type: " + response.getType() + " (expected: "
            + RPCMessage.Type.RPC_BLOCK_RESPONSE + ")");
      }
    } catch (Exception e) {
      LOG.error("exception in netty client: " + e + " message: " + e.getMessage());
    }
    return null;
  }
}
