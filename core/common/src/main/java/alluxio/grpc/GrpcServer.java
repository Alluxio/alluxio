/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import alluxio.retry.CountingRetry;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import io.grpc.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * An authenticated gRPC server. Corresponding gRPC channels to this server should be build by
 * {@link GrpcChannelBuilder}.
 */
public class GrpcServer {
  Server mServer;

  /**
   * Create a new instance of {@link GrpcServer}.
   *
   * @param server the wrapped server
   */
  public GrpcServer(Server server) {
    mServer = server;
  }

  /**
   * Start serving.
   *
   * @return this instance of {@link GrpcServer}
   * @throws IOException when unable to start serving
   */
  public GrpcServer start() throws IOException {
    RetryUtils.retry("Starting gRPC server", () -> mServer.start(),
        new ExponentialBackoffRetry(100, 500, 5));
    return this;
  }

  /**
   * @return the port that server is bound to, or null if the server is not bound to an address yet.
   */
  public int getBindPort() {
    return mServer.getPort();
  }

  /**
   * Stop serving.
   */
  public void shutdown() {
    mServer.shutdown();
  }

  /**
   * Stop serving immediately.
   */
  public void shutdownNow() {
    mServer.shutdownNow();
  }

  /**
   * Waits until the server is terminated.
   */
  public void awaitTermination() {
    while (!mServer.isTerminated()) {
      try {
        mServer.awaitTermination();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for gRPC server to terminate", e);
      }
    }
  }

  /**
   * Waits until the server is terminated.
   * 
   * @param timeout timeout duration to wait for termination
   * @param timeUnit time unit for given timeout value
   * @throws InterruptedException when interrupted
   * @return {@code true} if the server has been terminated
   */
  public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
    return mServer.awaitTermination(timeout, timeUnit);
  }

  /**
   * @return true if server is serving
   */
  public boolean isServing() {
    return !mServer.isShutdown() || !mServer.isTerminated();
  }
}
