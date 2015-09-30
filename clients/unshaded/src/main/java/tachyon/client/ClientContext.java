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

/**
 * A shared context in each client JVM. It provides common functionality such as the Tachyon
 * configuration and master address. All members of this class are immutable. This class is
 * thread safe.
 */
public final class ClientContext {
  private static final int CAPACITY = 10000;

  private static ExecutorService sExecutorService;
  /**
   * The static configuration object. There is only one TachyonConf object shared within the same
   * client.
   */
  private static TachyonConf sTachyonConf;

  private static InetSocketAddress sMasterAddress;

  private static Random sRandom;

  // private access to the reinitializer of BlockStoreContext
  private static BlockStoreContext.ReinitializerAccesser sBSCReinitializerAccesser =
      new BlockStoreContext.ReinitializerAccesser() {
        @Override
        public void receiveAccess(BlockStoreContext.PrivateReinitializer access) {
          sBSCReinitializer = access;
        }
      };
  private static BlockStoreContext.PrivateReinitializer sBSCReinitializer;

  // private access to the reinitializer of FileSystemContext
  private static FileSystemContext.ReinitializerAccesser sFSCReinitializerAccesser =
      new FileSystemContext.ReinitializerAccesser() {
        @Override
        public void receiveAccess(FileSystemContext.PrivateReinitializer access) {
          sFSCReinitializer = access;
        }
      };
  private static FileSystemContext.PrivateReinitializer sFSCReinitializer;

  static {
    sTachyonConf = new TachyonConf();

    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sRandom = new Random();

    sExecutorService = Executors.newFixedThreadPool(CAPACITY,
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
  }

  /**
   * @return the tachyonConf for the client process
   */
  public static TachyonConf getConf() {
    return sTachyonConf;
  }

  /**
   * @return the master address
   */
  public static InetSocketAddress getMasterAddress() {
    return sMasterAddress;
  }

  /**
   * @return a random non-negative long
   */
  public static long getRandomNonNegativeLong() {
    return Math.abs(sRandom.nextLong());
  }

  public static ExecutorService getExecutorService() {
    return sExecutorService;
  }
  /**
   * PrivateReinitializer can be used to reset the context. This access is limited only to classes
   * that implement ReinitializeAccess class.
   */
  public static class PrivateReinitializer {
    /**
     * Re-initializes the client context.
     *
     * @param conf new configuration to use
     */
    public synchronized void reinitializeWithConf(TachyonConf conf) {
      sTachyonConf = conf;
      String masterHostname =
          Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
      int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

      sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

      sRandom = new Random();
      sExecutorService.shutdown();
      sExecutorService = Executors.newFixedThreadPool(CAPACITY,
          ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));

      // the initialization is done lazily because BlockStoreContext and FileSystemContext need
      // ClientContext for class initialization
      if (sBSCReinitializer == null || sFSCReinitializer == null) {
        BlockStoreContext.INSTANCE.accessReinitializer(sBSCReinitializerAccesser);
        FileSystemContext.INSTANCE.accessReinitializer(sFSCReinitializerAccesser);
      }
      sBSCReinitializer.resetContext();
      sFSCReinitializer.resetContext();
    }
  }

  public interface ReinitializerAccesser {
    void receiveAccess(PrivateReinitializer access);
  }

  public static void accessReinitializer(ReinitializerAccesser accesser) {
    accesser.receiveAccess(new PrivateReinitializer());
  }

  private ClientContext() {}
}
