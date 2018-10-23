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
 * Interface representing an Alluxio process.
 */
public interface Process {

  /**
   * Starts the Alluxio process. This call blocks until the process is stopped via {@link #stop()}.
   * The {@link #waitForReady(int)} method can be used to make sure that the process is ready to
   * serve requests.
   */
  void start() throws Exception;

  /**
   * Stops the Alluxio process, blocking until the action is completed.
   */
  void stop() throws Exception;

  /**
   * Waits until the process is ready to serve requests.
   *
   * @param timeoutMs how long to wait in milliseconds
   * @return whether the process became ready before the specified timeout
   */
  // TODO(jiri): Replace with isServing.
  boolean waitForReady(int timeoutMs);
}
