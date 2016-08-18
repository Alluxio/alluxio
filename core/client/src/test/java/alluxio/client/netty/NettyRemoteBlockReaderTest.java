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

import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileWriteResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link NettyRemoteBlockReader} class.
 */
public class NettyRemoteBlockReaderTest {

  private NettyRemoteBlockReader mNettyRemoteBlockReader;
  private Bootstrap mBootstrap;
  private ClientHandler mClientHandler;

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
  public void before() throws InterruptedException {
    mBootstrap = Mockito.mock(Bootstrap.class);
    mClientHandler = new ClientHandler();
    mNettyRemoteBlockReader = new NettyRemoteBlockReader(mBootstrap, mClientHandler);

    ChannelFuture channelFuture = Mockito.mock(ChannelFuture.class);
    Channel channel = Mockito.mock(Channel.class);
    Mockito.when(channel.close()).thenReturn(channelFuture);
    Mockito.when(channelFuture.sync()).thenReturn(channelFuture);
    Mockito.when(channelFuture.channel()).thenReturn(channel);
    Mockito.when(mBootstrap.connect(Mockito.any(SocketAddress.class))).thenReturn(channelFuture);
  }

  /**
   * Test case for a valid {@link RPCBlockReadResponse} is received.
   */
  @Test
  public void readRemoteBlockTest() throws IOException {
    Thread channelThread = new Thread(new ChannelReadThread(
            RPCResponse.Type.RPC_BLOCK_READ_RESPONSE, RPCResponse.Status.SUCCESS
    ));
    channelThread.start();

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
  public void readRemoteBlockWithBadStatusTest() throws IOException {
    Thread channelThread = new Thread(new ChannelReadThread(
            RPCResponse.Type.RPC_BLOCK_READ_RESPONSE, RPCResponse.Status.BLOCK_LOCK_ERROR
    ));
    channelThread.start();

    mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);
  }

  /**
   * Test case for {@link RPCErrorResponse} is received.
   */
  @Test(expected = IOException.class)
  public void readRemoteBlockErrorResponseTest() throws IOException {
    Thread channelThread = new Thread(new ChannelReadThread(
            RPCResponse.Type.RPC_ERROR_RESPONSE, RPCResponse.Status.SUCCESS
    ));
    channelThread.start();
    mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);
  }

  /**
   * Test case for unexpected {@link RPCFileWriteResponse} is received.
   */
  @Test(expected = IOException.class)
  public void readRemoteBlockUnexpectedResponseTest() throws IOException {
    Thread channelThread = new Thread(new ChannelReadThread(
            RPCResponse.Type.RPC_FILE_READ_RESPONSE, RPCResponse.Status.SUCCESS
    ));
    channelThread.start();
    mNettyRemoteBlockReader.readRemoteBlock(INET_SOCKET_ADDRESS,
            BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID);
  }

  /**
   * Helper Thread for calling
   * {@link ClientHandler#channelRead0(ChannelHandlerContext, RPCMessage)}.
   */
  private class ChannelReadThread implements Runnable {
    private RPCResponse.Type mType;
    private RPCResponse.Status mStatus;

    public ChannelReadThread(RPCResponse.Type type, RPCResponse.Status status) {
      mType = type;
      mStatus = status;
    }

    @Override
    public void run() {
      for (int i = 0; i < 5; ++i) {
        try {
          switch (mType) {
            case RPC_BLOCK_READ_RESPONSE:
              mClientHandler.channelRead0(null, createRPCBlockReadResponse(mStatus));
              break;
            case RPC_ERROR_RESPONSE:
              mClientHandler.channelRead0(null, new RPCErrorResponse(mStatus));
              break;
            default:
              mClientHandler.channelRead0(null, new RPCFileWriteResponse(9876, 0, 20, mStatus));
              break;
          }
        } catch (IOException e) {
          // ignore
        }
        try {
          TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
  }

  private RPCBlockReadResponse createRPCBlockReadResponse(RPCResponse.Status status) {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) LENGTH);
    byteBuffer.put("alluxio".getBytes());
    DataBuffer dataBuffer = new DataByteBuffer(byteBuffer, LENGTH);
    return new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, dataBuffer, status);
  }
}
