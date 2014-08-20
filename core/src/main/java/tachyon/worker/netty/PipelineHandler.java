/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.netty;

import tachyon.worker.BlocksLocker;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Adds the block server's pipeline into the channel.
 */
public final class PipelineHandler extends ChannelInitializer<SocketChannel> {
  private final BlocksLocker LOCKER;
  private final EventExecutorGroup SYNC_GROUP;

  public PipelineHandler(BlocksLocker locker, EventExecutorGroup group) {
    LOCKER = locker;
    SYNC_GROUP = group;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("nioChunckedWriter", new ChunkedWriteHandler());
    pipeline.addLast("blockRequestDecoder", new BlockRequest.Decoder());
    pipeline.addLast("blockRequestEncoder", new BlockResponse.Encoder());
    pipeline.addLast(SYNC_GROUP, "dataServerHandler", new DataServerHandler(LOCKER));
  }
}
