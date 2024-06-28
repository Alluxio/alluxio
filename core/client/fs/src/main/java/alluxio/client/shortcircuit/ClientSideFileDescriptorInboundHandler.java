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

package alluxio.client.shortcircuit;

import alluxio.client.block.shortcircuit.OpenBlockMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.unix.FileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.SharedSecrets;

import java.util.concurrent.CountDownLatch;


public class ClientSideFileDescriptorInboundHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(ClientSideFileDescriptorInboundHandler.class);

  private OpenBlockMessage mOpenBlockMessage;
  private java.io.FileDescriptor mJavaFD;
  private CountDownLatch mLatch;

  public ClientSideFileDescriptorInboundHandler(OpenBlockMessage openBlockMessage, java.io.FileDescriptor fd,
      CountDownLatch latch) {
    this.mOpenBlockMessage = openBlockMessage;
    this.mJavaFD = fd;
    this.mLatch = latch;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof FileDescriptor) {
      int fd = ((FileDescriptor) msg).intValue();
      SharedSecrets.getJavaIOFileDescriptorAccess().set(mJavaFD, fd);
      mLatch.countDown();
    } else {
      LOG.error("Client channelRead data is Non-FD !");
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.channel().close();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(mOpenBlockMessage);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    mLatch.countDown();
    ctx.close();
  }
}
