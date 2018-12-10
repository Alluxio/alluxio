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

import io.grpc.Server;

import java.io.IOException;

/**
 * An authenticated gRPC server.
 * Corresponding gRPC channels to this server should be build by
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
    mServer = mServer.start();
    return this;
  }

  /**
   * @return the port that the server is binded to
   */
  public int getPort() {
    return mServer.getPort();
  }

  /**
   * Stop serving.
   */
  public void stop() {
    mServer.shutdown();
  }

  /**
   * Waits until the server is terminated.
   */
  public void awaitTermination() {
    while (!mServer.isTerminated()) {
      try {
        mServer.awaitTermination();
      } catch (InterruptedException e) {
        // TODO(ggezer) timeout
      }
    }
  }

  /**
   * @return true if server is serving
   */
  public boolean isServing(){
    return !mServer.isShutdown() && !mServer.isTerminated();
  }
}
