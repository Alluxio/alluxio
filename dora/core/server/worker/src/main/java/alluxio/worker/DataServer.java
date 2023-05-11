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

package alluxio.worker;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Defines how to interact with a server running the data protocol.
 */
public interface DataServer extends Closeable {

  /**
   * Gets the actual bind socket address. It is either a {@link InetSocketAddress} or a
   * {@link io.netty.channel.unix.DomainSocketAddress}.
   *
   * @return the bind socket address
   */
  SocketAddress getBindAddress();

  /**
   * Checks if the {@link DataServer} is closed.
   *
   * @return true if the {@link DataServer} is closed, false otherwise
   */
  boolean isClosed();

  /**
   * Waits for server to terminate.
   */
  void awaitTermination();
}
