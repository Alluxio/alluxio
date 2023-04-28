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
 * Test implementation of @SafeModeManager.
 */
public class TestSafeModeManager implements SafeModeManager {

  @Override
  public void notifyPrimaryMasterStarted() {
  }

  @Override
  public void notifyRpcServerStarted() {
  }

  @Override
  public boolean isInSafeMode() {
    return false;
  }
}
