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

package alluxio.client;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.network.NetworkAddressUtils;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A shared context in each client JVM. It provides common functionality such as the Alluxio
 * configuration and master address.
 */
@NotThreadSafe
public final class ClientContext {
  private static InetSocketAddress sMasterAddress;

  static {
    init();
  }

  /**
   * Initializes the client context singleton, bringing all non-Alluxio configuration state in sync
   * with the current Alluxio configuration.
   *
   * This method is useful for updating parts of {@link ClientContext} which depend on
   * {@link Configuration} when {@link Configuration} is changed, e.g. the master hostname or port.
   * This method requires that configuration has been initialized.
   */
  public static void init() {
    String masterHostname;
    if (Configuration.containsKey(PropertyKey.MASTER_HOSTNAME)) {
      masterHostname = Configuration.get(PropertyKey.MASTER_HOSTNAME);
    } else {
      masterHostname = NetworkAddressUtils.getLocalHostName();
    }
    int masterPort = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    MetricsSystem.startSinks();
  }

  /**
   * @return the master address
   */
  public static InetSocketAddress getMasterAddress() {
    return sMasterAddress;
  }

  private ClientContext() {} // prevent instantiation
}
