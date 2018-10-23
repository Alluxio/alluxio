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
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.dataserver.Protocol.Response;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.proto.ProtoMessage;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

/**
 * Test for {@link UnsupportedMessageHandler}.
 */
public class UnsupportedMessageHandlerTest {
  private EmbeddedChannel mChannel;

  @Before
  public void before() throws Exception {
    mChannel = new EmbeddedChannel(new UnsupportedMessageHandler());
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

  private Object waitForResponse(final EmbeddedChannel channel)
      throws TimeoutException, InterruptedException {
    return CommonUtils.waitForResult("response from the channel.", () -> channel.readOutbound(),
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }
}
