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

/**
 * Interface representing an Alluxio Server.
 */
public interface Server {

  /**
   * Starts the Alluxio server.
   *
   * @throws Exception if the server fails to start
   */
  void start() throws Exception;

  /**
   * Stops the Alluxio server. Here, anything created or started in {@link #start()} should be
   * cleaned up and shutdown.
   *
   * @throws Exception if the server fails to stop
   */
  void stop() throws Exception;
}
