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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.file.FileSystemContext;
import tachyon.client.table.RawTableContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.ClientMetrics;

/**
 * A shared context in each client JVM. It provides common functionality such as the Tachyon
 * configuration and master address.
 */
@ThreadSafe
public final class ClientContext {
  private static ExecutorService sExecutorService;
  private static TachyonConf sTachyonConf;
  private static InetSocketAddress sMasterAddress;
  private static ClientMetrics sClientMetrics;
  private static boolean sInitialized;

  static {
    sInitialized = false;
    reset();
  }

  /**
   * Initializes the client context singleton.
   */
  public static synchronized void reset() {
    reset(new TachyonConf());
  }

  /**
   * Initializes the client context singleton with a given conf.
   *
   * @param conf the configuration of Tachyon
   */
  public static synchronized void reset(TachyonConf conf) {
    sTachyonConf = conf;

    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_RPC_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);
    sClientMetrics = new ClientMetrics();

    if (sExecutorService != null) {
      sExecutorService.shutdown();
    }
    sExecutorService = Executors.newFixedThreadPool(
        sTachyonConf.getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS),
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
    // If this isn't the first time setting the ClientContext, we should reset other contexts so
    // that they can see the latest changes
    if (sInitialized) {
      BlockStoreContext.INSTANCE.reset();
      FileSystemContext.INSTANCE.reset();
      RawTableContext.INSTANCE.reset();
    }
    sInitialized = true;
  }

  /**
   * @return the {@link TachyonConf} for the client process
   */
  public static synchronized TachyonConf getConf() {
    checkContextInitialized();
    return sTachyonConf;
  }

  /**
   * @return the {@link ClientMetrics} for this client
   */
  public static synchronized ClientMetrics getClientMetrics() {
    checkContextInitialized();
    return sClientMetrics;
  }

  /**
   * @return the master address
   */
  public static synchronized InetSocketAddress getMasterAddress() {
    checkContextInitialized();
    return sMasterAddress;
  }

  /**
   * @return the executor service
   */
  public static synchronized ExecutorService getExecutorService() {
    checkContextInitialized();
    return sExecutorService;
  }

  private static void checkContextInitialized() {
    Preconditions.checkState(sInitialized, PreconditionMessage.CLIENT_CONTEXT_NOT_INITIALIZED);
  }

  private ClientContext() {} // prevent instantiation
}
