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

package alluxio.worker.grpc;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.grpc.GrpcServer;
import alluxio.util.grpc.GrpcServerBuilder;
import alluxio.util.network.NettyUtils;
import alluxio.worker.DataServer;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Runs a netty data server that responds to block requests.
 */
@NotThreadSafe
public final class GrpcDataServer implements DataServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataServer.class);

  private final SocketAddress mSocketAddress;
  private final long mTimeoutMs =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT);

  private GrpcServer mServer;

  private final int mThreadPoolSize;
  private final int mFlowControlWindow;

  /**
   * Creates a new instance of {@link GrpcDataServer}.
   *
   * @param address the server address
   * @param workerProcess the Alluxio worker process
   */
  public GrpcDataServer(final SocketAddress address, final WorkerProcess workerProcess) {
    int flowControlWindow = 256;
    mSocketAddress = address;
    mThreadPoolSize =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS) + 1;
    mFlowControlWindow = flowControlWindow;
    final Class<? extends ServerChannel> socketChannelClass = NettyUtils.getServerChannelClass(
        mSocketAddress instanceof DomainSocketAddress);

    ExecutorService executorService = Executors.newFixedThreadPool(mThreadPoolSize);
    try {
      mServer = GrpcServerBuilder.forAddress(address)
          .channelType(socketChannelClass)
          .addService(new BlockWorkerImpl(workerProcess))
          .executor(executorService)
          .flowControlWindow(mFlowControlWindow)
          .build()
          .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Server started, listening on " + address.toString());
  }

  @Override
  public void close() {
    if (mServer != null) {
      try {
        mServer.shutdown();
        mServer.awaitTermination(mTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Showdown interrupted", e);
      }
    }
  }

  @Override
  public SocketAddress getBindAddress() {
    return mSocketAddress;
  }

  @Override
  public boolean isClosed() {
    return mServer.isShutdown();
  }
}
