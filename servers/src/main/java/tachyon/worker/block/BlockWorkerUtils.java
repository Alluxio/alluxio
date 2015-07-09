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

package tachyon.worker.block;

import java.net.InetSocketAddress;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.NetworkUtils;

/**
 * Utils for tachyon.worker.block package
 */
public final class BlockWorkerUtils {

  /**
   * Gets the Tachyon master address from the configuration
   *
   * @param conf the configuration of Tachyon
   * @return the InetSocketAddress of the master
   */
  public static InetSocketAddress getMasterAddress(TachyonConf conf) {
    String masterHostname =
        conf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName(conf));
    int masterPort = conf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new InetSocketAddress(masterHostname, masterPort);
  }

  /**
   * Helper method to get the {@link java.net.InetSocketAddress} of the worker.
   * 
   * @param conf the configuration of Tachyon
   * @return the worker's address
   */
  public static InetSocketAddress getWorkerAddress(TachyonConf conf) {
    String workerHostname = NetworkUtils.getLocalHostName(conf);
    int workerPort = conf.getInt(Constants.WORKER_PORT, Constants.DEFAULT_WORKER_PORT);
    return new InetSocketAddress(workerHostname, workerPort);
  }
}
