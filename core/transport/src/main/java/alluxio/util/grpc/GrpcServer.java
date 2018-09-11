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
}
