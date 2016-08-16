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
import org.powermock.reflect.Whitebox;

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

  private InetSocketAddress mInetSocketAddress = new InetSocketAddress(1234);
  private long mBlockId = 4242L;
  private long mOffset = 1;
  private long mLength = 10;
  private long mLockId = 6;
  private long mSessionId = 3421;

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException {
    mNettyRemoteBlockReader = new NettyRemoteBlockReader();

    mBootstrap = Mockito.mock(Bootstrap.class);
    mClientHandler = new ClientHandler();

    Whitebox.setInternalState(mNettyRemoteBlockReader, "mClientBootstrap", mBootstrap);
    Whitebox.setInternalState(mNettyRemoteBlockReader, "mHandler", mClientHandler);

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

    ByteBuffer byteBuffer = mNettyRemoteBlockReader.readRemoteBlock(mInetSocketAddress,
            mBlockId, mOffset, mLength, mLockId, mSessionId);

    Assert.assertEquals(10, byteBuffer.capacity());
    Assert.assertEquals(10, byteBuffer.limit());
    byte[] dst = new byte[10];
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

    mNettyRemoteBlockReader.readRemoteBlock(mInetSocketAddress,
            mBlockId, mOffset, mLength, mLockId, mSessionId);
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
    mNettyRemoteBlockReader.readRemoteBlock(mInetSocketAddress,
            mBlockId, mOffset, mLength, mLockId, mSessionId);
  }

  /**
   * Test case for {@link RPCFileWriteResponse} is received.
   */
  @Test(expected = IOException.class)
  public void readRemoteBlockUnknownResponseTest() throws IOException {
    Thread channelThread = new Thread(new ChannelReadThread(
            RPCResponse.Type.RPC_FILE_WRITE_REQUEST, RPCResponse.Status.SUCCESS
    ));
    channelThread.start();
    mNettyRemoteBlockReader.readRemoteBlock(mInetSocketAddress,
            mBlockId, mOffset, mLength, mLockId, mSessionId);
  }

  /**
   * Helper Thread for calling
   * {@link ClientHandler#channelRead0(ChannelHandlerContext, RPCMessage)}.
   .   */
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
              mClientHandler.channelRead0(null, createRPCErrorResponse(mStatus));
              break;
            default:
              mClientHandler.channelRead0(null, createRPCFileWriteResponse(mStatus));
              break;
          }
        } catch (IOException e) {
          // ignore
        }
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
  }

  private RPCBlockReadResponse createRPCBlockReadResponse(RPCResponse.Status status) {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10);
    byteBuffer.put("alluxio".getBytes());
    DataBuffer dataBuffer = new DataByteBuffer(byteBuffer, 10);
    return new RPCBlockReadResponse(mBlockId, mOffset, mLength, dataBuffer, status);
  }

  private RPCFileWriteResponse createRPCFileWriteResponse(RPCResponse.Status status) {
    return new RPCFileWriteResponse(9876, 0, 20, status);
  }

  private RPCErrorResponse createRPCErrorResponse(RPCResponse.Status status) {
    return new RPCErrorResponse(status);
  }
}
