/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.netty;

import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Adds the block server's pipeline into the channel.
 */
@ThreadSafe
public final class PipelineHandler extends ChannelInitializer<SocketChannel> {
  private final DataServerHandler mDataServerHandler;

  /**
   * @param handler the handler for the main logic of the read path
   */
  public PipelineHandler(final DataServerHandler handler) {
    mDataServerHandler = handler;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("nioChunkedWriter", new ChunkedWriteHandler());
    pipeline.addLast("frameDecoder", RPCMessage.createFrameDecoder());
    pipeline.addLast("RPCMessageDecoder", new RPCMessageDecoder());
    pipeline.addLast("RPCMessageEncoder", new RPCMessageEncoder());
    pipeline.addLast("dataServerHandler", mDataServerHandler);
  }
}
