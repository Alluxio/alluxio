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

package alluxio.master.journal.raft.transport;

import io.atomix.catalyst.concurrent.ThreadContext;

/**
 * {@link CopycatGrpcConnection} implementation for server.
 */
public class CopycatGrpcServerConnection extends CopycatGrpcConnection {
  /**
   * Creates a connection object for server.
   *
   * Note: {@link #setTargetObserver} should be called explicitly before using the connection.
   *
   * @param context copycat thread context
   * @param requestTimeoutMs timeout in milliseconds for requests
   */
  public CopycatGrpcServerConnection(ThreadContext context, long requestTimeoutMs) {
    super(ConnectionOwner.SERVER, context, requestTimeoutMs);
  }
}
