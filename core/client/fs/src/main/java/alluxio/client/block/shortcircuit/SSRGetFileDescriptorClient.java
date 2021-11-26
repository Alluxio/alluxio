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

package alluxio.client.block.shortcircuit;

import alluxio.client.shortcircuit.ClientSideFileDescriptorInboundHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketReadMode;
import io.netty.channel.unix.UnixChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SSRGetFileDescriptorClient {

  private static final Logger LOG = LoggerFactory.getLogger(SSRGetFileDescriptorClient.class);

  private OpenBlockMessage mOpenBlockMessage;
  private FileDescriptor mJavaFd;
  private String mDomainSocketPath;
  private EventLoopGroup mWorkerGroup;
  private CountDownLatch mLatch;

  public SSRGetFileDescriptorClient(OpenBlockMessage openBlockMessage, FileDescriptor javaFD, 
      String domainSocketPath, EventLoopGroup mWorkerGroup, CountDownLatch latch) {
    this.mOpenBlockMessage = openBlockMessage;
    this.mJavaFd = javaFD;
    this.mDomainSocketPath = domainSocketPath;
    this.mWorkerGroup = mWorkerGroup;
    this.mLatch = latch;
  }

  public void start() {
    Bootstrap bootstrap = new Bootstrap();
    try {
      bootstrap.group(mWorkerGroup)
          .channel(EpollDomainSocketChannel.class)
          .option(UnixChannelOption.DOMAIN_SOCKET_READ_MODE, DomainSocketReadMode.FILE_DESCRIPTORS)
          .handler(new ChannelInitializer<EpollDomainSocketChannel>() {
            @Override
            protected void initChannel(EpollDomainSocketChannel socketChannel) throws Exception {
              socketChannel.pipeline()
                  .addFirst(new OpenBlockMsgEncoder(mOpenBlockMessage))
                  .addLast(new ClientSideFileDescriptorInboundHandler(mOpenBlockMessage, mJavaFd, mLatch));
            }
          });
      
      ChannelFuture f = bootstrap.connect(new DomainSocketAddress(mDomainSocketPath)).sync();
      f.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      mWorkerGroup.shutdownGracefully(0, 0, TimeUnit.MICROSECONDS);
    }
  }
}
