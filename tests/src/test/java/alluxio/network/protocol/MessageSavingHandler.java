/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.network.protocol;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.junit.Assert;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This is a simple handler for the channel pipelines. This simply saves the message it receives.
 * The stored message can be retrieved with {@link #getMessage()} which is blocking until the
 * message is received.
 *
 * This is for encoder/decoder testing in {@link RPCMessageIntegrationTest}.
 */
@ChannelHandler.Sharable
public class MessageSavingHandler extends SimpleChannelInboundHandler<RPCMessage> {
  public RPCMessage mMessage = null;
  private Semaphore mMessageAvailable = new Semaphore(0);

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCMessage message) {
    mMessage = message;
    mMessageAvailable.release();
  }

  // Returns the received message. This is blocking.
  public RPCMessage getMessage() {
    try {
      if (!mMessageAvailable.tryAcquire(1, 1, TimeUnit.SECONDS)) {
        Assert.fail("Timed out receiving message.");
      }
    } catch (InterruptedException e) {
      Assert.fail("Failed with exception: " + e.getMessage());
    }
    return mMessage;
  }

  public void reset() {
    mMessage = null;
    mMessageAvailable = new Semaphore(0);
  }
}
