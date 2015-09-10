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

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.file.FileSystemContext;
import tachyon.conf.TachyonConf;
import tachyon.worker.ClientMetrics;

/**
 * A shared context in each client JVM. It provides common functionality such as the Tachyon
 * configuration and master address. All members of this class are immutable. This class is
 * thread safe.
 */
public class ClientContext {
  /**
   * The static configuration object. There is only one TachyonConf object shared within the same
   * client.
   */
  private static TachyonConf sTachyonConf;

  private static InetSocketAddress sMasterAddress;

  private static ClientMetrics sClientMetrics;

  private static Random sRandom;

  static {
    sTachyonConf = new TachyonConf();

    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sClientMetrics = new ClientMetrics();

    sRandom = new Random();
  }

  /**
   * @return the tachyonConf for the client process
   */
  public static TachyonConf getConf() {
    return sTachyonConf;
  }

  /**
   * @return the ClientMetrics for this client
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
   * @return a random non-negative long
   */
  public static long getRandomNonNegativeLong() {
    return Math.abs(sRandom.nextLong());
  }

  /**
   * This method is only for testing purposes.
   *
   * @param conf new configuration to use
   */
  // TODO(calvin): Find a better way to handle testing configurations
  public static synchronized void reinitializeWithConf(TachyonConf conf) {
    sTachyonConf = conf;
    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sRandom = new Random();

    BlockStoreContext.INSTANCE.resetContext();
    FileSystemContext.INSTANCE.resetContext();
  }
}
