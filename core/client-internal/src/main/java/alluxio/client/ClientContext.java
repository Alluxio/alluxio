/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.ClientMetrics;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A shared context in each client JVM. It provides common functionality such as the Alluxio
 * configuration and master address.
 */
@ThreadSafe
public final class ClientContext {
  private static ExecutorService sExecutorService;
  private static Configuration sConf;
  private static InetSocketAddress sMasterAddress;
  private static ClientMetrics sClientMetrics;

  static {
    reset();
  }

  /**
   * Resets to the default Alluxio configuration and initializes the client context singleton.
   *
   * This method is useful for undoing changes to {@link Configuration} made by unit tests.
   */
  private static void reset() {
    sConf = new Configuration();
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
    String masterHostname = Preconditions.checkNotNull(sConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sConf.getInt(Constants.MASTER_RPC_PORT);
    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sClientMetrics = new ClientMetrics();

    sExecutorService = Executors.newFixedThreadPool(
        sConf.getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS),
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
  }

  /**
   * @return the {@link Configuration} for the client process
   */
  public static Configuration getConf() {
    return sConf;
  }

  /**
   * @return the {@link ClientMetrics} for this client
   */
  public static ClientMetrics getClientMetrics() {
    return sClientMetrics;
  }

  /**
   * @return the master address
   */
  public static InetSocketAddress getMasterAddress() {
    return sMasterAddress;
  }

  /**
   * @return the executor service
   */
  public static ExecutorService getExecutorService() {
    return sExecutorService;
  }

  private ClientContext() {} // prevent instantiation
}
