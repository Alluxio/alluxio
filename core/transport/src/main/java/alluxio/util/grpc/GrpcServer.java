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

package alluxio.util.grpc;

import io.grpc.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A simple wrapper around the {@link Server} class in grpc. Outside of this module, this
 * class should be used to replace references to {@link Server} for dependency management.
 * Note: This class is intended for internal use only.
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
    mServer = mServer.start();
    return this;
  }

  /**
   * Start shutdown server and reject new requests.
   */
  public void shutdown() {
    mServer.shutdown();
  }

  /**
   * Wait for server to be terminated or until timeout is reached.
   *
   * @param timeout the maximum amount of time to wait until giving up
   * @param unit time unit for the timeout
   * @return whether the server is terminated
   * @throws InterruptedException
   */
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return mServer.awaitTermination(timeout, unit);
  }

  /**
   * @return whether the server is shutdown
   */
  public boolean isShutdown() {
    return mServer.isShutdown();
  }


  /**
   * @return the address the server is bound to, or null if the server is not bound to an inet
   *         socket address
   */
  public SocketAddress getBindAddress() {
    int port = mServer.getPort();
    return port >= 0 ? new InetSocketAddress(mServer.getPort()) : null;
  }
}
