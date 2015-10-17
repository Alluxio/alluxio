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

package tachyon.client;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.file.FileSystemContext;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.ClientMetrics;

/**
 * A shared context in each client JVM. It provides common functionality such as the Tachyon
 * configuration and master address. This class is thread safe.
 */
public final class ClientContext {

  private static ExecutorService sExecutorService;
  private static TachyonConf sTachyonConf;
  private static InetSocketAddress sMasterAddress;
  private static ClientMetrics sClientMetrics;
  private static Random sRandom;
  private static boolean sInitialized;

  static {
    sInitialized = false;
    reset();
  }

  /**
   * Initializes the client context singleton.
   */
  public static synchronized void reset() {
    if (sInitialized) {
      return;
    }
    reset(new TachyonConf());
  }

  /**
   * Initializes the client context singleton with a given conf.
   */
  public static synchronized void reset(TachyonConf conf) {
    sTachyonConf = conf;

    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);
    sClientMetrics = new ClientMetrics();
    sRandom = new Random();

    if (sExecutorService != null) {
      sExecutorService.shutdown();
    }
    sExecutorService = Executors.newFixedThreadPool(
        sTachyonConf.getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS),
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
    // We must set sInitialized to true before resetting BlockStoreContext and FileSystemContext
    // as they need ClientContext initialized.
    sInitialized = true;
    BlockStoreContext.INSTANCE.reset();
    FileSystemContext.INSTANCE.reset();
  }

  /**
   * @return the tachyonConf for the client process
   */
  public static synchronized TachyonConf getConf() {
    Preconditions.checkState(sInitialized, "Client Context not initialized.");
    return sTachyonConf;
  }

  /**
   * @return the ClientMetrics for this client
   */
  public static synchronized ClientMetrics getClientMetrics() {
    Preconditions.checkState(sInitialized, "Client Context not initialized.");
    return sClientMetrics;
  }

  /**
   * @return the master address
   */
  public static synchronized InetSocketAddress getMasterAddress() {
    Preconditions.checkState(sInitialized, "Client Context not initialized.");
    return sMasterAddress;
  }

  /**
   * @return a random non-negative long
   */
  public static synchronized long getRandomNonNegativeLong() {
    Preconditions.checkState(sInitialized, "Client Context not initialized.");
    return Math.abs(sRandom.nextLong());
  }

  public static synchronized ExecutorService getExecutorService() {
    Preconditions.checkState(sInitialized, "Client Context not initialized.");
    return sExecutorService;
  }

  private ClientContext() {}
}
