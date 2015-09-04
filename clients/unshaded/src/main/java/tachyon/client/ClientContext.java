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

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.block.BSContext;
import tachyon.client.file.FSContext;
import tachyon.conf.TachyonConf;

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

  private static SessionMasterClientPool sSessionClientPool;

  static {
    sTachyonConf = new TachyonConf();

    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sSessionClientPool = new SessionMasterClientPool(sMasterAddress, sTachyonConf);
  }

  /**
   * Returns the one and only static {@link TachyonConf} object which is shared among all classes
   * within the client
   *
   * @return the tachyonConf for the client process
   */
  public static TachyonConf getConf() {
    return sTachyonConf;
  }

  /**
   * Returns the {@link InetSocketAddress} of the master node.
   *
   * @return the master address
   */
  public static InetSocketAddress getMasterAddress() {
    return sMasterAddress;
  }

  public static SessionMasterClient acquireSessionMasterClient() {
    return sSessionClientPool.acquire();
  }

  public static void releaseSessionMasterClient(SessionMasterClient userMasterClient) {
    sSessionClientPool.release(userMasterClient);
  }

  /**
   * This method is only for testing purposes
   *
   * @param conf new configuration to use
   */
  // TODO: Find a better way to handle testing confs
  public static synchronized void reinitializeWithConf(TachyonConf conf) {
    sSessionClientPool.close();
    sTachyonConf = conf;
    String masterHostname = Preconditions.checkNotNull(sTachyonConf.get(Constants.MASTER_HOSTNAME));
    int masterPort = sTachyonConf.getInt(Constants.MASTER_PORT);

    sMasterAddress = new InetSocketAddress(masterHostname, masterPort);

    sSessionClientPool = new SessionMasterClientPool(sMasterAddress, sTachyonConf);

    BSContext.INSTANCE.resetContext();
    FSContext.INSTANCE.resetContext();
  }
}
