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

package alluxio.worker.http;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

/**
 * {@link HttpServerInitializer} is used for initializing the Netty pipeline of HTTP Server.
 */
public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

  private final PagedService mPagedService;

  /**
   * {@link HttpServerInitializer} is used for initializing the Netty pipeline of HTTP Server.
   * @param pagedService the {@link PagedService} object provides page related RESTful API
   */
  @Inject
  public HttpServerInitializer(PagedService pagedService) {
    mPagedService = pagedService;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    ChannelPipeline p = ch.pipeline();
    p.addLast(new HttpServerCodec());
    p.addLast(new HttpObjectAggregator(1024 * 10));
    p.addLast(new HttpServerExpectContinueHandler());
    p.addLast(new HttpServerHandler(mPagedService));
  }
}
