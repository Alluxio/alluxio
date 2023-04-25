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

package alluxio.dora.worker;

import alluxio.worker.DataWorker;

import static java.util.Objects.requireNonNull;

import java.net.InetSocketAddress;

/**
 * Factory for data server.
 */
public interface DataServerFactory {
  DataServer createRemoteGrpcDataServer(DataWorker dataWorker);

  DataServer createDomainSocketDataServer(DataWorker worker);

  /**
   * Get gRPC bind address.
   *
   * @return the InetSocketAddress object with gRPC bind address
   */
  InetSocketAddress getGRpcBindAddress();

  /**
   * Get gRPC connect address.
   *
   * @return the InetSocketAddress object with gRPC connect address
   */
  InetSocketAddress getConnectAddress();
}
