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

package alluxio.client.lineage;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A shared context in each client JVM for common lineage master client functionality such as a pool
 * of lineage master clients. Any remote clients will be created and destroyed on a per use basis.
 */
@ThreadSafe
public enum LineageContext {
  INSTANCE;

  private LineageMasterClientPool mLineageMasterClientPool;

  /**
   * Creates a new lineage context.
   */
  LineageContext() {
    reset();
  }

  /**
   * Acquires a lineage master client from the lineage master client pool.
   *
   * @return the acquired lineage master client
   */
  public LineageMasterClient acquireMasterClient() {
    return mLineageMasterClientPool.acquire();
  }

  /**
   * Releases a lineage master client into the lineage master client pool.
   *
   * @param masterClient a lineage master client to release
   */
  public void releaseMasterClient(LineageMasterClient masterClient) {
    mLineageMasterClientPool.release(masterClient);
  }

  /**
   * Re-initializes the {@link LineageContext}.
   */
  public void reset() {
    if (mLineageMasterClientPool != null) {
      mLineageMasterClientPool.close();
    }

    String masterHostname;
    if (Configuration.containsKey(PropertyKey.MASTER_HOSTNAME)) {
      masterHostname = Configuration.get(PropertyKey.MASTER_HOSTNAME);
    } else {
      masterHostname = NetworkAddressUtils.getLocalHostName();
    }
    int masterPort = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
    InetSocketAddress masterAddress = new InetSocketAddress(masterHostname, masterPort);
    mLineageMasterClientPool = new LineageMasterClientPool(masterAddress);
  }
}
