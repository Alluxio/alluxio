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

package tachyon.client.next;

import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;

import tachyon.Constants;
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
  private static final TachyonConf TACHYON_CONF;

  private static final InetSocketAddress MASTER_ADDRESS;

  static {
    TACHYON_CONF = new TachyonConf();

    String masterHostname = Preconditions.checkNotNull(TACHYON_CONF.get(Constants.MASTER_HOSTNAME));
    int masterPort = TACHYON_CONF.getInt(Constants.MASTER_PORT);

    MASTER_ADDRESS = new InetSocketAddress(masterHostname, masterPort);
  }

  /**
   * Returns the one and only static {@link TachyonConf} object which is shared among all classes
   * within the client
   *
   * @return the tachyonConf for the worker process
   */
  public static TachyonConf getConf() {
    return TACHYON_CONF;
  }

  public static InetSocketAddress getMasterAddress() {
    return MASTER_ADDRESS;
  }
}
