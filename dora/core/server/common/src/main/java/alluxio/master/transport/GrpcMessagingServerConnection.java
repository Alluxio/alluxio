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

package alluxio.master.transport;

import java.util.concurrent.ExecutorService;

/**
 * {@link GrpcMessagingConnection} implementation for server.
 */
public class GrpcMessagingServerConnection extends GrpcMessagingConnection {

  /**
   * Creates a messaging connection for server.
   *
   * Note: {@link #setTargetObserver} should be called explicitly before using the connection.
   *
   * @param transportId transport Id for this connection
   * @param context grpc messaging thread context
   * @param executor transport executor
   * @param requestTimeoutMs timeout in milliseconds for requests
   */
  public GrpcMessagingServerConnection(String transportId, GrpcMessagingContext context,
      ExecutorService executor, long requestTimeoutMs) {
    super(ConnectionOwner.SERVER, transportId, context, executor, requestTimeoutMs);
  }
}
