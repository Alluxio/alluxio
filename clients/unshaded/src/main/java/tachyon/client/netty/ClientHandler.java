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
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import tachyon.Constants;
import tachyon.network.protocol.RPCBlockResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCResponse;

/**
 * This handles all the messages received by the client channel.
 */
@ChannelHandler.Sharable
public final class ClientHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * The interface for listeners to implement to receive callbacks when messages are received.
   */
  public interface ResponseListener {
    /** This method will be called when a message is received on the client. */
    void onResponseReceived(RPCResponse response);
  }

  private final HashSet<ResponseListener> mListeners;

  public ClientHandler() {
    mListeners = new HashSet<ResponseListener>(4);
  }

  public void addListener(ResponseListener listener) {
    mListeners.add(listener);
  }

  public void removeListener(ResponseListener listener) {
    mListeners.remove(listener);
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    if (msg instanceof RPCResponse) {
      handleResponse(ctx, (RPCResponse) msg);
    } else {
      // The client should only receive RPCResponse messages.
      throw new IllegalArgumentException("No handler implementation for rpc message type: "
          + msg.getType());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }

  private void handleResponse(final ChannelHandlerContext ctx, final RPCResponse resp)
      throws IOException {
    for (ResponseListener listener : mListeners) {
      listener.onResponseReceived(resp);
    }
  }
}
