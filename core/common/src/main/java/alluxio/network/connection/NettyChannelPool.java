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

import alluxio.network.connection.ConnectionPool;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class NettyChannelPool extends ConnectionPool<Channel> {
  // Every 5 mins.
  private static final int GC_INTERVAL_IN_SECS = 300;
  private static final int HEARTBEAT_EXECUTOR_SIZE = 2;
  private static final int HEARTBEAT_INTERVAL_IN_SECS = 300;

  public NettyChannelPool(int maxConnections) {
    super(maxConnections, Executors.newFixedThreadPool(HEARTBEAT_EXECUTOR_SIZE),
        HEARTBEAT_INTERVAL_IN_SECS, GC_INTERVAL_IN_SECS);
  }

  @Override
  protected void closeConnection(Channel channel) {
    channel.close();
  }

  @Override
  protected void closeConnectionSync(Channel channel) {
    channel.close().syncUninterruptibly();
  }

  @Override
  protected Channel createNewConnection() {

  }
}