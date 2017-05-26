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
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.dataserver.Protocol.Response;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Function;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link DataServerUnsupportedMessageHandler}.
 */
public class DataServerUnsupportedMessageHandlerTest {
  private EmbeddedChannel mChannel;

  @Before
  public void before() throws Exception {
    mChannel = new EmbeddedChannel(new DataServerUnsupportedMessageHandler());
  }

  /**
   * Tests a protobuf message with the unimplemented status code is returned when a protobuf message
   * reaches the unsupported message handler.
   */
  @Test
  public void testProtoUnsupported() throws Exception {
    RPCProtoMessage inboundRequest =
        new RPCProtoMessage(new ProtoMessage(Protocol.ReadRequest.newBuilder().build()));
    mChannel.writeInbound(inboundRequest);
    Object response = waitForResponse(mChannel);
    Assert.assertTrue(response instanceof RPCProtoMessage);
    RPCProtoMessage protoResponse = (RPCProtoMessage) response;
    Assert.assertNull(protoResponse.getPayloadDataBuffer());
    Response r = protoResponse.getMessage().asResponse();
    Assert.assertEquals(PStatus.UNIMPLEMENTED, r.getStatus());
  }

  /**
   * Tests a rpc message with the unknown message status is returned when a non-protobuf rpc message
   * reaches the unsupported message handler.
   */
  @Test
  public void testNonProtoUnsupported() throws Exception {
    RPCMessage inboundRequest = new RPCBlockReadRequest(0, 0, 0, 0, 0);
    mChannel.writeInbound(inboundRequest);
    Object response = waitForResponse(mChannel);
    Assert.assertTrue(response instanceof RPCErrorResponse);
    RPCErrorResponse errorResponse = (RPCErrorResponse) response;
    Assert.assertEquals(RPCResponse.Status.UNKNOWN_MESSAGE_ERROR, errorResponse.getStatus());
  }

  private Object waitForResponse(final EmbeddedChannel channel) {
    return CommonUtils
        .waitForResult("response from the channel.", new Function<Void, Object>() {
          @Override
          public Object apply(Void v) {
            return channel.readOutbound();
          }
        }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }
}
