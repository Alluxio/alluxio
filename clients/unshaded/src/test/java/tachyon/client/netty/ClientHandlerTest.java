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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.netty.channel.ChannelHandlerContext;

import tachyon.exception.ExceptionMessage;
import tachyon.network.protocol.RPCBlockReadRequest;
import tachyon.network.protocol.RPCBlockReadResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCResponse;
import tachyon.network.protocol.databuffer.DataBuffer;

public class ClientHandlerTest {

  private ClientHandler mHandler;
  private ChannelHandlerContext mContext;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mHandler = new ClientHandler();
    mContext = Mockito.mock(ChannelHandlerContext.class);
  }

  /**
   * Makes sure that a {@link NullPointerException} is thrown if a listener is added which is null.
   */
  @Test
  public void addListenerTest() {
    mThrown.expect(NullPointerException.class);

    mHandler.addListener(null);
  }

  /**
   * Makes sure that the response is received as expected.
   *
   * @throws IOException
   */
  @Test
  public void channelRead0ResponseReceivedTest() throws IOException {
    final ClientHandler.ResponseListener listener =
        Mockito.mock(ClientHandler.ResponseListener.class);
    final DataBuffer buffer = Mockito.mock(DataBuffer.class);
    final RPCResponse response = new RPCBlockReadResponse(0, 0, 0, buffer,
        RPCResponse.Status.SUCCESS);

    mHandler.addListener(listener);
    mHandler.channelRead0(mContext, response);

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
    final RPCMessage message = new RPCBlockReadRequest(0, 0, 0);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.NO_RPC_HANDLER.getMessage(message.getType()));

    mHandler.channelRead0(mContext, message);
  }

  /**
   * Makes sure that the {@link ChannelHandlerContext} is closed.
   *
   * @throws Exception
   */
  @Test
  public void exceptionCaughtClosesContextTest() throws Exception {
    mHandler.exceptionCaught(mContext, new Throwable());

    Mockito.verify(mContext, Mockito.times(1)).close();
  }
}
