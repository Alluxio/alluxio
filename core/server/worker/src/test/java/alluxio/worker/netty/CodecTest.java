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

package alluxio.worker.netty;

import alluxio.Constants;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Function;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public final class CodecTest {
  private EmbeddedChannel mChannel;

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    mChannel = new EmbeddedChannel(RPCMessage.createFrameDecoder(), new RPCMessageDecoder(),
        new RPCMessageEncoder());
  }

  @Test
  public void readRequest() throws Exception {
    mChannel.writeOutbound(
        new RPCProtoMessage(new ProtoMessage(Protocol.ReadRequest.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isReadRequest()));
  }

  @Test
  public void writeRequest() throws Exception {
    mChannel.writeOutbound(
        new RPCProtoMessage(new ProtoMessage(Protocol.WriteRequest.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isWriteRequest()));
  }

  @Test
  public void response() throws Exception {
    mChannel.writeOutbound(
        new RPCProtoMessage(new ProtoMessage(Protocol.Response.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isResponse()));
  }

  @Test
  public void localBlockOpenRequest() throws Exception {
    mChannel.writeOutbound(
        new RPCProtoMessage(new ProtoMessage(Protocol.LocalBlockOpenRequest.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isLocalBlockOpenRequest()));
  }

  @Test
  public void localBlockOpenResponse() throws Exception {
    mChannel.writeOutbound(new RPCProtoMessage(
        new ProtoMessage(Protocol.LocalBlockOpenResponse.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isLocalBlockOpenResponse()));
  }

  @Test
  public void localBlockCloseRequest() throws Exception {
    mChannel.writeOutbound(new RPCProtoMessage(
        new ProtoMessage(Protocol.LocalBlockCloseRequest.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isLocalBlockCloseRequest()));
  }

  @Test
  public void localBlockCreateRequest() throws Exception {
    mChannel.writeOutbound(new RPCProtoMessage(
        new ProtoMessage(Protocol.LocalBlockCreateRequest.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isLocalBlockCreateRequest()));
  }

  @Test
  public void localBlockCreateResponse() throws Exception {
    mChannel.writeOutbound(new RPCProtoMessage(
        new ProtoMessage(Protocol.LocalBlockCreateResponse.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isLocalBlockCreateResponse()));
  }

  @Test
  public void localBlockCompleteRequest() throws Exception {
    mChannel.writeOutbound(new RPCProtoMessage(
        new ProtoMessage(Protocol.LocalBlockCompleteRequest.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isLocalBlockCompleteRequest()));
  }

  @Test
  public void heartbeat() throws Exception {
    mChannel.writeOutbound(
        new RPCProtoMessage(new ProtoMessage(Protocol.Heartbeat.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isHeartbeat()));
  }

  @Test
  public void readResponse() throws Exception {
    mChannel.writeOutbound(
        new RPCProtoMessage(new ProtoMessage(Protocol.ReadResponse.getDefaultInstance())));
    Object object = waitForOneResponse(mChannel);
    mChannel.writeInbound(object);
    Object request = waitForOneRequest(mChannel);
    Assert.assertTrue(
        request instanceof RPCProtoMessage && (((RPCProtoMessage) request).getMessage()
            .isReadResponse()));
  }

  /**
   * Waits for one response.
   *
   * @return the response
   */
  private Object waitForOneResponse(final EmbeddedChannel channel) {
    return CommonUtils.waitForResult("response from the channel", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readOutbound();
      }
    }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }

  /**
   * Waits for one request.
   *
   * @return the request
   */
  private Object waitForOneRequest(final EmbeddedChannel channel) {
    return CommonUtils.waitForResult("response from the channel", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readInbound();
      }
    }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }
}
