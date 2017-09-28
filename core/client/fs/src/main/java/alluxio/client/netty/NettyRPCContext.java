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

import io.netty.channel.Channel;

/**
 * Context to send a netty RPC.
 */
public final class NettyRPCContext {
  /** The netty channel, default to be null. */
  private Channel mChannel;
  /** The RPC timeout in ms, default to +inf. */
  private long mTimeoutMs = Long.MAX_VALUE;

  private NettyRPCContext() {}

  /**
   * @return the default context
   */
  public static NettyRPCContext defaults() {
    return new NettyRPCContext();
  }

  /**
   * @return the channel
   */
  public Channel getChannel() {
    return mChannel;
  }

  /**
   * @return the timeout
   */
  public long getTimeoutMs() {
    return mTimeoutMs;
  }

  /**
   * @param channel the channel
   * @return updated context
   */
  public NettyRPCContext setChannel(Channel channel) {
    mChannel = channel;
    return this;
  }

  /**
   * @param timeoutMs the timeout in ms
   * @return updated context
   */
  public NettyRPCContext setTimeout(long timeoutMs) {
    mTimeoutMs = timeoutMs;
    return this;
  }
}
