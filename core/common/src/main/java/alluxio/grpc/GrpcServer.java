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

package alluxio.grpc;

import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import alluxio.security.authentication.AuthenticationServer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.io.Closer;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * An authenticated gRPC server. Corresponding gRPC channels to this server should be build by
 * {@link GrpcChannelBuilder}.
 */
public final class GrpcServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  /** Internal server reference. */
  private Server mServer;
  /** Authentication server for this server. */
  private AuthenticationServer mAuthServer;
  /** Closer for closing resources during shutdown. */
  private Closer mCloser;

  /** Set to TRUE when the server has been successfully started. **/
  private boolean mStarted = false;
  private final long mServerShutdownTimeoutMs;

  /**
   * Create a new instance of {@link GrpcServer}.
   *
   * @param server the wrapped server
   * @param authServer the authentication server
   * @param closer resources to close during shutting down of this server
   * @param serverShutdownTimeoutMs server shutdown timeout in milliseconds
   */
  public GrpcServer(Server server, AuthenticationServer authServer, Closer closer,
      long serverShutdownTimeoutMs) {
    mServer = server;
    mAuthServer = authServer;
    mCloser = closer;
    mServerShutdownTimeoutMs = serverShutdownTimeoutMs;
  }

  /**
   * @return the authentication server associated with this server
   */
  @VisibleForTesting
  public AuthenticationServer getAuthenticationServer() {
    return mAuthServer;
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
   * @return the port that server is bound to, or null if the server is not bound to an address yet
   */
  public int getBindPort() {
    return mServer.getPort();
  }

  /**
   * Shuts down the server.
   *
   * @return {@code true} if the server was successfully terminated
   * @throws InterruptedException
   */
  public boolean shutdown() {
    // Stop accepting new connections.
    mServer.shutdown();
    // Close resources that potentially owns active streams.
    try {
      mCloser.close();
    } catch (IOException e) {
      LOG.error("Failed to close resources during shutdown.", e);
      // Do nothing.
    }
    // Force shutdown remaining calls.
    mServer.shutdownNow();
    // Wait until server terminates.
    try {
      return mServer.awaitTermination(mServerShutdownTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("InternalServer", mServer)
        .add("AuthServerType", mAuthServer.getClass().getSimpleName())
        .toString();
  }
}
