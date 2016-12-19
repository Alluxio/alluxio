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

import alluxio.client.file.FileSystemContext;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileWriteResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * Tests for the {@link NettyRemoteBlockReader} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(NettyClient.class)
public class NettyRemoteBlockReaderTest {

  private FileSystemContext mContext;
  private NettyRemoteBlockReader mNettyRemoteBlockReader;
  private static Bootstrap sBootstrap = Mockito.mock(Bootstrap.class);
  private static ClientHandler sClientHandler = new ClientHandler();
  private Channel mChannel;
  private ChannelFuture mChannelFuture;
  private ChannelPipeline mChannelPipeline;

  private static final InetSocketAddress INET_SOCKET_ADDRESS = new InetSocketAddress(1234);
  private static final long BLOCK_ID = 4242L;
  private static final long OFFSET = 1;
  private static final long LENGTH = 10;
  private static final long LOCK_ID = 6;
  private static final long SESSION_ID = 3421;

  /**
   * Set up.
   */
  @Before
  public void before() throws Exception {
    PowerMockito.mockStatic(NettyClient.class);
    BDDMockito.given(NettyClient.createClientBootstrap()).willReturn(sBootstrap);
    mContext = PowerMockito.mock(FileSystemContext.class);
    mNettyRemoteBlockReader = new NettyRemoteBlockReader(mContext);
    mChannel = Mockito.mock(Channel.class);
    Mockito.when(mContext.acquireNettyChannel(Mockito.any(InetSocketAddress.class)))
        .thenReturn(mChannel);
    mChannelFuture = Mockito.mock(ChannelFuture.class);
    mChannelPipeline = Mockito.mock(ChannelPipeline.class);

    Mockito.when(mChannel.close()).thenReturn(mChannelFuture);
    Mockito.when(mChannelFuture.sync()).thenReturn(mChannelFuture);
    Mockito.when(mChannelFuture.channel()).thenReturn(mChannel);
    Mockito.when(mChannelFuture.isDone()).thenReturn(true);
    Mockito.when(mChannelFuture.isSuccess()).thenReturn(true);
    Mockito.when(sBootstrap.connect(Mockito.any(SocketAddress.class))).thenReturn(mChannelFuture);
    Mockito.when(sBootstrap.connect()).thenReturn(mChannelFuture);
    Mockito.when(sBootstrap.clone()).thenReturn(sBootstrap);
    Mockito.when(sBootstrap.remoteAddress(Mockito.any(InetSocketAddress.class)))
        .thenReturn(sBootstrap);
    Mockito.when(mChannel.pipeline()).thenReturn(mChannelPipeline);
    Mockito.when(mChannelPipeline.last()).thenReturn(sClientHandler);
  }

  /**
   * Test case for a valid {@link RPCBlockReadResponse} is received.
   */
  @Test
  public void readRemoteBlock() throws IOException {
    Mockito.when(mChannel.writeAndFlush(Mockito.any())).then(new Answer<ChannelFuture>() {
      @Override
      public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
        sClientHandler.channelRead0(null, createRPCBlockReadResponse(RPCResponse.Status.SUCCESS));
        return mChannelFuture;
      }
    });

    ByteBuffer byteBuffer = mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);

    Assert.assertEquals(LENGTH, byteBuffer.capacity());
    Assert.assertEquals(LENGTH, byteBuffer.limit());
    byte[] dst = new byte[(int) LENGTH];
    byteBuffer.get(dst);
    Assert.assertEquals("alluxio", new String(dst).substring(0, 7));
  }

  /**
   * Test case for an invalid {@link RPCBlockReadResponse} is received.
   */
  @Test(expected = IOException.class)
  public void readRemoteBlockWithBadStatus() throws IOException {
    Mockito.when(mChannel.writeAndFlush(Mockito.any())).then(new Answer<ChannelFuture>() {
      @Override
      public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
        sClientHandler.channelRead0(null,
                createRPCBlockReadResponse(RPCResponse.Status.UFS_READ_FAILED));
        return mChannelFuture;
      }
    });

    mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);
  }

  /**
   * Test case for {@link RPCErrorResponse} is received.
   */
  @Test(expected = IOException.class)
  public void readRemoteBlockErrorResponse() throws IOException {
    Mockito.when(mChannel.writeAndFlush(Mockito.any())).then(new Answer<ChannelFuture>() {
      @Override
      public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
        sClientHandler.channelRead0(null, new RPCErrorResponse(RPCResponse.Status.SUCCESS));
        return mChannelFuture;
      }
    });

    mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);
  }

  /**
   * Test case for unexpected {@link RPCFileWriteResponse} is received.
   */
  @Test(expected = IOException.class)
  public void readRemoteBlockUnexpectedResponse() throws IOException {
    Mockito.when(mChannel.writeAndFlush(Mockito.any())).then(new Answer<ChannelFuture>() {
      @Override
      public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
        sClientHandler.channelRead0(null,
                new RPCFileWriteResponse(9876, 0, 20, RPCResponse.Status.SUCCESS));
        return mChannelFuture;
      }
    });

    mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);
  }

  private RPCBlockReadResponse createRPCBlockReadResponse(RPCResponse.Status status) {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) LENGTH);
    byteBuffer.put("alluxio".getBytes());
    DataBuffer dataBuffer = new DataByteBuffer(byteBuffer, LENGTH);
    return new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, dataBuffer, status);
  }
}
