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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link HttpServer} provides Alluxio RESTful API. It is implemented through Netty.
 */
public final class HttpServer {

  private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
  private static final boolean SSL = false;

  private static final int PORT = Configuration.getInt(PropertyKey.WORKER_HTTP_SERVER_PORT);

  private final ExecutorService mHttpServerThreadPool = Executors.newFixedThreadPool(1);

  private final HttpServerInitializer mHttpServerInitializer;

  /**
   * {@link HttpServer} provides Alluxio RESTful API. It is implemented through Netty.
   * @param httpServerInitializer this object initializes the Netty pipeline of HTTP Server
   */
  @Inject
  public HttpServer(HttpServerInitializer httpServerInitializer) {
    mHttpServerInitializer = httpServerInitializer;
  }

  /**
   * Starts the HTTP server.
   */
  public void start() {
    mHttpServerThreadPool.submit(this::startHttpServer);
  }

  private void startHttpServer() {
    // Configure the server.
    int threadsNum = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
    EventLoopGroup bossGroup = new NioEventLoopGroup(threadsNum);
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.option(ChannelOption.SO_BACKLOG, 1024);
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(mHttpServerInitializer);

      Channel ch = b.bind(PORT).sync().channel();

      LOG.info("Open your web browser and navigate to "
          + (SSL ? "https" : "http") + "://127.0.0.1:" + PORT + '/');

      ch.closeFuture().sync();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }
}
