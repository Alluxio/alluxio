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
  private static ExecutorService sBlockClientExecutorService;
  private static ExecutorService sFileClientExecutorService;
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
    Configuration.clientInit();
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
    String masterHostname =
        Preconditions.checkNotNull(Configuration.get(Constants.MASTER_HOSTNAME));
    int masterPort = Configuration.getInt(Constants.MASTER_RPC_PORT);
    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sClientMetrics = new ClientMetrics();

    sBlockClientExecutorService = Executors
        .newFixedThreadPool(Configuration.getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS),
            ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
    sFileClientExecutorService = Executors
        .newFixedThreadPool(Configuration.getInt(Constants.USER_FILE_WORKER_CLIENT_THREADS),
            ThreadFactoryUtils.build("file-worker-heartbeat-%d", true));
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
   * @return the executor service for block clients
   */
  public static ExecutorService getBlockClientExecutorService() {
    return sBlockClientExecutorService;
  }

  /**
   * @return the executor service for file clients
   */
  public static ExecutorService getFileClientExecutorService() {
    return sFileClientExecutorService;
  }

  private ClientContext() {} // prevent instantiation
}
