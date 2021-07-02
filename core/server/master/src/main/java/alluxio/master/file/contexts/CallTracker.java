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
   * @return the type of call tracker
   */
  Type getType();

  /**
   * Defines call-tracker types.
   */
  enum Type {
    GRPC_CLIENT_TRACKER,
    STATE_LOCK_TRACKER
  }
}
