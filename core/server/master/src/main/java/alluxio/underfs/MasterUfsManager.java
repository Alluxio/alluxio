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

package alluxio.underfs;

import alluxio.util.network.NetworkAddressUtils;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class manages the UFS used by different services.
 */
@ThreadSafe
public final class MasterUfsManager extends AbstractUfsManager {

  /**
   * Establishes the connection to the given UFS from master.
   *
   * @param ufs UFS instance
   * @throws IOException if failed to create the UFS instance
   */
  public void connect(UnderFileSystem ufs) throws IOException {
    ufs.connectFromMaster(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC));
  }

  /**
   * Constructs the instance of {@link MasterUfsManager}.
   */
  public MasterUfsManager() {}

}
