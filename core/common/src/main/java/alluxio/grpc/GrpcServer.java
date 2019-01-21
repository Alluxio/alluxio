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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * An authenticated gRPC server. Corresponding gRPC channels to this server should be build by
 * {@link GrpcChannelBuilder}.
 */
public final class GrpcServer {
  private Server mServer;

  /** Set to TRUE when the server has been successfully started. **/
  private boolean mStarted = false;

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
    mStarted = true;
    return this;
  }

  /**
   * @return the port that server is bound to, or null if the server is not bound to an address yet.
   */
  public int getBindPort() {
    return mServer.getPort();
  }

  /**
   * Shuts down the server.
   *
   * @return {@code true} if the server was successfully terminated.
   * @throws InterruptedException
   */
  public boolean shutdown() {
    mServer.shutdown();
    try {
      return mServer.awaitTermination(
          Configuration.getMs(PropertyKey.MASTER_GRPC_SERVER_SHUTDOWN_TIMEOUT),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    } finally {
      mServer.shutdownNow();
    }
  }

  /**
   * Waits until the server is terminated.
   */
  public void awaitTermination() {
    try {
      mServer.awaitTermination();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      // Allow thread to exit.
    }
  }

  /**
   * @return true if server is serving
   */
  public boolean isServing() {
    return mStarted && !mServer.isShutdown() || !mServer.isTerminated();
  }
}
