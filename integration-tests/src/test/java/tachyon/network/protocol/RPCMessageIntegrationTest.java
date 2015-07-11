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

package tachyon.network.protocol;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import tachyon.Constants;
import tachyon.TestUtils;
import tachyon.network.protocol.databuffer.DataByteBuffer;
import tachyon.network.protocol.databuffer.DataFileChannel;
import tachyon.util.NetworkUtils;

/**
 * This tests the encoding and decoding of RPCMessage's. This is done by setting up a simple
 * client/server bootstrap connection, and writing messages on the client side, and verifying it on
 * the server side. In this case, the server simply stores the message received, and does not reply
 * to the client.
 */
public class RPCMessageIntegrationTest {
  private static final long USER_ID = 10;
  private static final long BLOCK_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 33;

  // This channel initializer sets up a simple pipeline with the encoder and decoder.
  private static class PipelineInitializer extends ChannelInitializer<SocketChannel> {
    private MessageSavingHandler mHandler = null;

    public PipelineInitializer(MessageSavingHandler handler) {
      mHandler = handler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast("frameDecoder", RPCMessage.createFrameDecoder());
      pipeline.addLast("RPCMessageDecoder", new RPCMessageDecoder());
      pipeline.addLast("RPCMessageEncoder", new RPCMessageEncoder());
      pipeline.addLast("handler", mHandler);
    }
  }

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private Channel mOutgoingChannel;

  private static NioEventLoopGroup sEventClient;
  private static NioEventLoopGroup sEventServer;
  private static MessageSavingHandler sIncomingHandler;
  private static Bootstrap sBootstrapClient;
  private static SocketAddress sLocalAddress;

  @BeforeClass
  public static void beforeClass() {
    sEventClient = new NioEventLoopGroup(1);
    sEventServer = new NioEventLoopGroup(1);
    sIncomingHandler = new MessageSavingHandler();

    // Setup the server.
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(sEventServer);
    bootstrap.channel(NioServerSocketChannel.class);
    bootstrap.childHandler(new PipelineInitializer(sIncomingHandler));

    InetSocketAddress address =
        new InetSocketAddress(NetworkUtils.getLocalHostName(100), Constants.DEFAULT_MASTER_PORT);
    ChannelFuture cf = bootstrap.bind(address).syncUninterruptibly();
    sLocalAddress = cf.channel().localAddress();

    // Setup the client.
    sBootstrapClient = new Bootstrap();
    sBootstrapClient.group(sEventClient);
    sBootstrapClient.channel(NioSocketChannel.class);
    sBootstrapClient.handler(new PipelineInitializer(new MessageSavingHandler()));
  }

  @AfterClass
  public static void afterClass() {
    // Shut everything down.
    sEventClient.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    sEventServer.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    sEventClient.terminationFuture().syncUninterruptibly();
    sEventServer.terminationFuture().syncUninterruptibly();
  }

  @Before
  public final void before() {
    sIncomingHandler.reset();

    // Connect to the server.
    ChannelFuture cf = sBootstrapClient.connect(sLocalAddress).syncUninterruptibly();
    mOutgoingChannel = cf.channel();
  }

  @After
  public final void after() {
    // Close the client connection.
    mOutgoingChannel.close().syncUninterruptibly();
  }

  private void assertValid(RPCBlockRequest expected, RPCBlockRequest actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getBlockId(), actual.getBlockId());
    Assert.assertEquals(expected.getOffset(), actual.getOffset());
    Assert.assertEquals(expected.getLength(), actual.getLength());
  }

  private void assertValid(RPCBlockResponse expected, RPCBlockResponse actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getBlockId(), actual.getBlockId());
    Assert.assertEquals(expected.getOffset(), actual.getOffset());
    Assert.assertEquals(expected.getLength(), actual.getLength());
    Assert.assertEquals(expected.getStatus(), actual.getStatus());
    if (expected.getLength() == 0) {
      // Length is 0, so payloads should be null.
      Assert.assertNull(expected.getPayloadDataBuffer());
      Assert.assertNull(actual.getPayloadDataBuffer());
    } else {
      Assert.assertTrue(TestUtils.equalIncreasingByteBuffer((int) OFFSET, (int) LENGTH, actual
          .getPayloadDataBuffer().getReadOnlyByteBuffer()));
    }
  }

  private void assertValid(RPCBlockWriteRequest expected, RPCBlockWriteRequest actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getBlockId(), actual.getBlockId());
    Assert.assertEquals(expected.getOffset(), actual.getOffset());
    Assert.assertEquals(expected.getLength(), actual.getLength());
    Assert.assertEquals(expected.getUserId(), actual.getUserId());
    if (expected.getLength() > 0) {
      Assert.assertTrue(TestUtils.equalIncreasingByteBuffer((int) OFFSET, (int) LENGTH, actual
          .getPayloadDataBuffer().getReadOnlyByteBuffer()));
    }
  }

  private void assertValid(RPCBlockWriteResponse expected, RPCBlockWriteResponse actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getBlockId(), actual.getBlockId());
    Assert.assertEquals(expected.getOffset(), actual.getOffset());
    Assert.assertEquals(expected.getLength(), actual.getLength());
    Assert.assertEquals(expected.getUserId(), actual.getUserId());
    Assert.assertEquals(expected.getStatus(), actual.getStatus());
  }

  private void assertValid(RPCErrorResponse expected, RPCErrorResponse actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getStatus(), actual.getStatus());
  }

  // Returns a FileChannel for a temporary file, filled with test data.
  private FileChannel getTempFileChannel() throws IOException {
    // Create a temporary file for the FileChannel.
    File f = mFolder.newFile("temp.txt");
    String path = f.getAbsolutePath();

    FileOutputStream os = new FileOutputStream(path);
    os.write(TestUtils.getIncreasingByteArray((int) (OFFSET + LENGTH)));
    os.close();

    return new FileInputStream(f).getChannel();
  }

  // This encodes and decodes the 'msg' by sending it through the client and server pipelines.
  private RPCMessage encodeThenDecode(RPCMessage msg) {
    // Write the message to the outgoing channel.
    mOutgoingChannel.writeAndFlush(msg);
    // Read the decoded message from the incoming side.
    RPCMessage outputMessage = sIncomingHandler.getMessage();
    return outputMessage;
  }

  @Test
  public void RPCBlockRequestTest() {
    RPCBlockRequest msg = new RPCBlockRequest(BLOCK_ID, OFFSET, LENGTH);
    RPCBlockRequest decoded = (RPCBlockRequest) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCBlockResponseTest() {
    ByteBuffer payload = TestUtils.getIncreasingByteBuffer((int) OFFSET, (int) LENGTH);
    RPCBlockResponse msg =
        new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, new DataByteBuffer(payload, LENGTH),
            RPCResponse.Status.SUCCESS);
    RPCBlockResponse decoded = (RPCBlockResponse) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCBlockResponseEmptyPayloadTest() {
    RPCBlockResponse msg = new RPCBlockResponse(BLOCK_ID, OFFSET, 0, null,
        RPCResponse.Status.SUCCESS);
    RPCBlockResponse decoded = (RPCBlockResponse) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCBlockResponseErrorTest() {
    RPCBlockResponse msg = RPCBlockResponse.createErrorResponse(new RPCBlockRequest(BLOCK_ID,
        OFFSET, LENGTH), RPCResponse.Status.FILE_DNE);
    RPCBlockResponse decoded = (RPCBlockResponse) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCBlockResponseFileChannelTest() throws IOException {
    FileChannel payload = getTempFileChannel();
    RPCBlockResponse msg =
        new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH,
            new DataFileChannel(payload, OFFSET, LENGTH), RPCResponse.Status.SUCCESS);
    RPCBlockResponse decoded = (RPCBlockResponse) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCBlockWriteRequestTest() {
    ByteBuffer payload = TestUtils.getIncreasingByteBuffer((int) OFFSET, (int) LENGTH);
    RPCBlockWriteRequest msg = new RPCBlockWriteRequest(USER_ID, BLOCK_ID, OFFSET, LENGTH,
        new DataByteBuffer(payload, LENGTH));
    RPCBlockWriteRequest decoded = (RPCBlockWriteRequest) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCBlockWriteResponseTest() {
    RPCBlockWriteResponse msg =
        new RPCBlockWriteResponse(USER_ID, BLOCK_ID, OFFSET, LENGTH, RPCResponse.Status.SUCCESS);
    RPCBlockWriteResponse decoded = (RPCBlockWriteResponse) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCErrorResponseTest() {
    for (RPCResponse.Status status : RPCResponse.Status.values()) {
      RPCErrorResponse msg = new RPCErrorResponse(status);
      RPCErrorResponse decoded = (RPCErrorResponse) encodeThenDecode(msg);
      assertValid(msg, decoded);
    }
  }
}
