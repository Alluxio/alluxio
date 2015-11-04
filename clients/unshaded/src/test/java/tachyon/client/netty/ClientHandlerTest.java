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

package tachyon.client.netty;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.netty.channel.ChannelHandlerContext;

import tachyon.network.protocol.RPCBlockReadRequest;
import tachyon.network.protocol.RPCBlockReadResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCResponse;
import tachyon.network.protocol.databuffer.DataBuffer;

public class ClientHandlerTest {

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /**
   * Makes sure that a {@link NullPointerException} is thrown if a listener is added which is null.
   */
  @Test
  public void addListenerTest() {
    mThrown.expect(NullPointerException.class);

    ClientHandler handler = new ClientHandler();
    handler.addListener(null);
  }

  /**
   * Makes sure that the response is received as expected.
   *
   * @throws IOException
   */
  @Test
  public void channelRead0ResponseReceivedTest() throws IOException {
    ClientHandler handler = new ClientHandler();
    final ClientHandler.ResponseListener listener =
        Mockito.mock(ClientHandler.ResponseListener.class);
    final ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
    DataBuffer buffer = Mockito.mock(DataBuffer.class);
    final RPCResponse response = new RPCBlockReadResponse(0, 0, 0, buffer,
        RPCResponse.Status.SUCCESS);

    handler.addListener(listener);
    handler.channelRead0(context, response);

    Mockito.verify(listener, Mockito.times(1)).onResponseReceived(response);
  }

  /**
   * Makes sure that an {@link IllegalArgumentException} is thrown when the message is
   * not a {@link tachyon.network.protocol.RPCResponse}.
   *
   * @throws IOException
   */
  @Test
  public void channelRead0ThrowsExceptionTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("No handler implementation for rpc message type: RPC_BLOCK_READ_REQUEST");

    ClientHandler handler = new ClientHandler();
    final ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
    final RPCMessage message = new RPCBlockReadRequest(0, 0, 0);

    handler.channelRead0(context, message);
  }

  /**
   * Makes sure that the {@link ChannelHandlerContext} is closed.
   *
   * @throws Exception
   */
  @Test
  public void exceptionCaughtClosesContextTest() throws Exception {
    ClientHandler handler = new ClientHandler();
    ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);

    handler.exceptionCaught(context, new Throwable());

    Mockito.verify(context, Mockito.times(1)).close();
  }
}
