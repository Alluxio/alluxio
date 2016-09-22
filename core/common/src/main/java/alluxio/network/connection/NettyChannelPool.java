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

package alluxio.network.connection;

import alluxio.Constants;
import alluxio.resource.DynamicResourcePool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.IOException;

/**
 * A pool to manage netty channels.
 */
public final class NettyChannelPool extends DynamicResourcePool<Channel> {
  private Bootstrap mBootstrap;
  private final int mGcThresholdInSecs;

  /**
   * Creates a netty channel pool instance with a minimum capacity of 1.
   *
   * @param bootstrap the netty bootstrap used to create netty channel
   * @param maxCapacity the maximum capacity of the pool
   * @param gcThresholdInSecs when a channel is older than this threshold and the pool's capacity
   *        is above the minimum capaicty (1), it is closed and removed from the pool.
   */
  public NettyChannelPool(Bootstrap bootstrap, int maxCapacity, int gcThresholdInSecs) {
    super(Options.defaultOptions().setMaxCapacity(maxCapacity));
    mBootstrap = bootstrap;
    mGcThresholdInSecs = gcThresholdInSecs;
  }

  @Override
  protected void closeResource(Channel channel) {
    LOG.info("Channel closed");
    channel.close();
  }

  @Override
  protected void closeResourceSync(Channel channel) {
    LOG.info("Channel closed synchronously.");
    channel.close().syncUninterruptibly();
  }

  /**
   * Creates a netty channel instance.
   *
   * @return the channel created
   * @throws IOException if it fails to create a channel
   */
  @Override
  protected Channel createNewResource() throws IOException {
    Bootstrap bs = mBootstrap.clone();
    bs.clone();
    try {
      ChannelFuture channelFuture = bs.connect().sync();
      if (channelFuture.isSuccess()) {
        LOG.info("Created netty channel to with netty boostrap {}.", mBootstrap.toString());
        return channelFuture.channel();
      } else {
        LOG.error("Failed to create netty channel with netty boostrap {} and error {}.",
            mBootstrap.toString(), channelFuture.cause().getMessage());
        throw new IOException(channelFuture.cause());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks whether a channel is healthy.
   *
   * @param channel the channel to check
   * @return true if the channel is active (i.e. connected)
   */
  @Override
  protected boolean isHealthy(Channel channel) {
    return channel.isActive();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<Channel> channelResourceInternal) {
    return System.currentTimeMillis() - channelResourceInternal
        .getLastAccessTimeMs() > mGcThresholdInSecs * Constants.SECOND_MS;
  }
}
