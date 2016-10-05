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

import alluxio.Configuration;
import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A pool to manage netty channels. Netty has it own implementation of channel pool but that
 * doesn't fit our needs for several reasons:
 * 1. We need a dynamic pool which can garbage collect idle channels.
 * 2. We need to have control on how channel is created. For example, our channel handler might
 *    not be annotated with {@code Sharable}. So we need to deep copy handlers when creating new
 *    channels.
 * 3. Netty channel pool interface is async which is not necessary for our usecase.
 */
@ThreadSafe
public final class NettyChannelPool extends DynamicResourcePool<Channel> {
  private static final int NETTY_CHANNEL_POOL_GC_THREADPOOL_SIZE = 10;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(NETTY_CHANNEL_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("NettyChannelPoolGcThreads-%d", true));
  private static final boolean POOL_DISABLED =
      Configuration.getBoolean(alluxio.PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED);
  private Callable<Bootstrap> mBootstrap;
  private final long mGcThresholdMs;

  /**
   * Creates a netty channel pool instance with a minimum capacity of 1.
   *
   * @param bootstrap the netty bootstrap used to create netty channel
   * @param maxCapacity the maximum capacity of the pool
   * @param gcThresholdMs when a channel is older than this threshold and the pool's capacity
   *        is above the minimum capacity(1), it is closed and removed from the pool.
   */
  public NettyChannelPool(Callable<Bootstrap> bootstrap, int maxCapacity, long gcThresholdMs) {
    super(Options.defaultOptions().setMaxCapacity(maxCapacity).setGcExecutor(GC_EXECUTOR));
    mBootstrap = bootstrap;
    mGcThresholdMs = gcThresholdMs;
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
    Bootstrap bs;
    try {
      bs = mBootstrap.call();
    } catch (Exception e) {
      // No exception should happen here.
      throw Throwables.propagate(e);
    }
    try {
      ChannelFuture channelFuture = bs.connect().sync();
      if (channelFuture.isSuccess()) {
        LOG.info("Created netty channel to with netty boostrap {}.", mBootstrap);
        return channelFuture.channel();
      } else {
        LOG.error("Failed to create netty channel with netty boostrap {} and error {}.",
            mBootstrap, channelFuture.cause().getMessage());
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
    if (POOL_DISABLED) {
      // If we always return false here, channels acquired by NettyChannelPool#acquire() will always
      // be newly created channel. With this feature turned on, >= 1.3.0 client will be backward
      // compatible with <= 1.2.0 server.
      return false;
    }
    return channel.isActive();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<Channel> channelResourceInternal) {
    return System.currentTimeMillis() - channelResourceInternal
        .getLastAccessTimeMs() > mGcThresholdMs;
  }
}
