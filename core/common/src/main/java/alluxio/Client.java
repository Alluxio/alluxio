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

package alluxio;

import alluxio.exception.ConnectionFailedException;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for a client in the Alluxio system.
 */
public interface Client extends Closeable {

  /**
   * Connects with the remote.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  void connect() throws IOException, ConnectionFailedException;

  /**
   * Closes the connection, then queries and sets current remote address.
   */
  void resetConnection();

  /**
   * Returns the connected status of the client.
   *
   * @return true if this client is connected to the remote
   */
  boolean isConnected();

  /**
   * Closes the connection with the remote permanently. This instance should be not be reused after
   * closing.
   */
  void close();
}
