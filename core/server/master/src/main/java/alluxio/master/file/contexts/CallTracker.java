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

package alluxio.master.file.contexts;

/**
 * An interface for RPC level call tracking.
 */
public interface CallTracker {
  /**
   * @return {@code true} if call is cancelled by the client
   */
  boolean isCancelled();

  /**
   * Used when call tracking should not be enabled.
   * Invoking it will throw a runtime exception.
   *
   * This tracker will be used as default for service implementations that are not modified
   * for tracking. That way using the tracking functionality without making proper
   * modifications will throw exceptions.
   */
  CallTracker DISABLED_TRACKER = () -> {
    throw new IllegalStateException("Call tracking is not supported.");
  };

  /**
   * Used when call tracking is not desired.
   * It will always return @{code false}.
   *
   * This tracker will be used during testing for service implementations that use tracking.
   */
  CallTracker NOOP_TRACKER = () -> false;
}
