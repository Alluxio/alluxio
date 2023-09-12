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

import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;

public class UnsupportedMessageHandlerTest {
  UnsupportedMessageHandler mHandler;
  ChannelHandlerContext mCtx;
  RPCProtoMessage mMsg;


  @Before
  public void before() {
    mHandler = new UnsupportedMessageHandler();
    mCtx = mock(ChannelHandlerContext.class);
    when(mCtx.writeAndFlush(any())).thenReturn(null);
    when(mCtx.fireChannelRead(any())).thenReturn(null);

    Protocol.WriteRequest t = Protocol.WriteRequest.newBuilder()
      .setId(0)
      .setCreateUfsFileOptions(Protocol.CreateUfsFileOptions.newBuilder().setUfsPath("test"))
      .build();;
    mMsg = new RPCProtoMessage(new ProtoMessage(t));
  }

  @Test
  public void testChannelRead() throws Exception {
    mHandler.channelRead(mCtx, mMsg);
    verify(mCtx).writeAndFlush(any());
    verify(mCtx, times(0)).fireChannelRead(any());

    mHandler.channelRead(mCtx, null);
    verify(mCtx).fireChannelRead(any());
  }
}
