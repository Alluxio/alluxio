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
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handles all the messages received by the client channel.
 */
@NotThreadSafe
public final class ClientHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(ClientHandler.class);

  /**
   * The interface for listeners to implement to receive callbacks when messages are received.
   */
  public interface ResponseListener {
    /**
     * This method will be called when a message is received on the client.
     *
     * @param response the RPC response
     */
    void onResponseReceived(RPCResponse response);

    /**
     * This method will be called when an exception is caught on the client.
     *
     * @param cause the cause
     */
    void onExceptionCaught(Throwable cause);
  }

  private final Set<ResponseListener> mListeners;

  /**
   * Creates a new {@link ClientHandler}.
   */
  public ClientHandler() {
    mListeners = new HashSet<>(4);
  }

  /**
   * Adds a {@link ResponseListener} listener to the client handler.
   *
   * @param listener the listener to add
   * @return the current handler
   */
  public ClientHandler addListener(ResponseListener listener) {
    mListeners.add(Preconditions.checkNotNull(listener, "listener"));
    return this;
  }

  /**
   * Removes all {@link ResponseListener} listeners from the client handler.
   */
  public void removeListeners() {
    mListeners.clear();
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    if (msg instanceof RPCResponse) {
      handleResponse((RPCResponse) msg);
    } else {
      // The client should only receive RPCResponse messages.
      throw new IllegalArgumentException(ExceptionMessage.NO_RPC_HANDLER.getMessage(msg.getType()));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    for (ResponseListener listener : mListeners) {
      listener.onExceptionCaught(cause);
    }
  }

  private void handleResponse(final RPCResponse resp) {
    for (ResponseListener listener : mListeners) {
      listener.onResponseReceived(resp);
    }
  }
}
