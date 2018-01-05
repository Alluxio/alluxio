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

package alluxio.master;

/**
 * Interface for managing safe mode state.
 */
public interface SafeModeManager {

  /**
   * Notifies {@link SafeModeManager} that the primary master is started.
   */
  void notifyPrimaryMasterStarted();

  /**
   * Notifies {@link SafeModeManager} that the RPC server is started.
   */
  void notifyRpcServerStarted();

  /**
   * @return whether master is in safe mode
   */
  boolean isInSafeMode();
}
