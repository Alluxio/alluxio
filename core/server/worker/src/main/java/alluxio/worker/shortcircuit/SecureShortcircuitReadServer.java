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

package alluxio.worker.shortcircuit;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.WorkerProcess;
import com.esotericsoftware.minlog.Log;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Secure Shortcircuit Read Server is used to obtain an FileDescriptor object
 * of a specific file given the blockId.
 */
public class SecureShortcircuitReadServer {

  private static final Logger LOG = LoggerFactory.getLogger(SecureShortcircuitReadServer.class);

  private EventLoopGroup mBossGroup;
  private EventLoopGroup mWorkerGroup;
  private final WorkerProcess mAlluxioWorkerProcess;

  
  public SecureShortcircuitReadServer(EventLoopGroup connectGroup, EventLoopGroup workGroup, 
      final WorkerProcess workerProcess) {
    this.mBossGroup = connectGroup;
    this.mWorkerGroup = workGroup;
    this.mAlluxioWorkerProcess = workerProcess;
  }

  public void start() {
    new Thread(() -> {
      try {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(mBossGroup, mWorkerGroup)
            .channel(EpollServerDomainSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(new ChannelInitializer<EpollDomainSocketChannel>() {
              @Override
              protected void initChannel(EpollDomainSocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("FDOutBoundHandler", new FileDescriptorOutboundHandler())
                        .addLast("OpenBlockInBoundHandler",
                             new OpenBlockRequestInboundHandler(mAlluxioWorkerProcess));
              }
            });

        LOG.info("Starting secure shortcircuit read server...");
        String domainSocketPath =
            ServerConfiguration.get(PropertyKey.WORKER_SECURE_SHORT_CIRCUIT_READ_DOMAIN_SOCKET_ADDRESS);
        ChannelFuture sync = bootstrap.bind(new DomainSocketAddress(domainSocketPath)).sync();
        LOG.info("Secure shortcircuit read server starts successfully.");
        sync.channel().closeFuture().sync();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        Log.info("Secure shortcircuit read server stopped successfully.");
        mBossGroup.shutdownGracefully();
        mWorkerGroup.shutdownGracefully();
      }
    }).start();
  }
}
