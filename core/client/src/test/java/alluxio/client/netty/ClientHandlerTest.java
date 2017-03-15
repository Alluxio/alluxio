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

import alluxio.exception.ExceptionMessage;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Tests for the {@link ClientHandler} class.
 */
public class ClientHandlerTest {

  private ClientHandler mHandler;
  private ChannelHandlerContext mContext;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the handler before a test runs.
   */
  @Before
  public void before() {
    mHandler = new ClientHandler();
    mContext = Mockito.mock(ChannelHandlerContext.class);
  }

  /**
   * Makes sure that a {@link NullPointerException} is thrown if a listener is added which is null.
   */
  @Test
  public void addListener() {
    mThrown.expect(NullPointerException.class);

    mHandler.addListener(null);
  }

  /**
   * Makes sure that the response is received as expected.
   */
  @Test
  public void channelRead0ResponseReceived() throws IOException {
    final ClientHandler.ResponseListener listener =
        Mockito.mock(ClientHandler.ResponseListener.class);
    final DataBuffer buffer = Mockito.mock(DataBuffer.class);
    final RPCResponse response = new RPCBlockReadResponse(0, 0, 0, buffer,
        RPCResponse.Status.SUCCESS);

    mHandler.addListener(listener);
    mHandler.channelRead0(mContext, response);

    Mockito.verify(listener).onResponseReceived(response);
  }

  /**
   * Makes sure that an {@link IllegalArgumentException} is thrown when the message is
   * not a {@link alluxio.network.protocol.RPCResponse}.
   */
  @Test
  public void channelRead0ThrowsException() throws IOException {
    final RPCMessage message = new RPCBlockReadRequest(0, 0, 0, 0, 0);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.NO_RPC_HANDLER.getMessage(message.getType()));

    mHandler.channelRead0(mContext, message);
  }

  /**
   * Makes sure that the exceptions in the handler is handled properly.
   */
  @Test
  public void exceptionCaughtClosesContext() throws Exception {
    SingleResponseListener singleResponseListener = new SingleResponseListener();
    mHandler.addListener(singleResponseListener);
    mHandler.exceptionCaught(mContext, new IOException());

    boolean exceptionCaught = false;
    try {
      singleResponseListener.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        exceptionCaught = true;
      }
    }
    Assert.assertTrue(exceptionCaught);
  }
}
