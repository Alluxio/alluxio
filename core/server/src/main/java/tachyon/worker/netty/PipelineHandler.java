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

package tachyon.worker.netty;

import javax.annotation.concurrent.ThreadSafe;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;

import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCMessageDecoder;
import tachyon.network.protocol.RPCMessageEncoder;

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
