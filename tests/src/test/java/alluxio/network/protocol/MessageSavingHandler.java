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
