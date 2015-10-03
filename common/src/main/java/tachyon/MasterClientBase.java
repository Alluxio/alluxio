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

package tachyon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.conf.TachyonConf;
import tachyon.util.network.NetworkAddressUtils;

/**
 * The base class for master clients.
 */
public abstract class MasterClientBase extends ClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected final boolean mUseZookeeper;

  /**
   * Creates a new master client base.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
  public MasterClientBase(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf, "master");
    mUseZookeeper = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER);
  }

  /**
   * Returns the {@link InetSocketAddress} of the master. If zookeeper is used, this will consult
   * the zookeeper instance for the master address.
   *
   * @return the {@link InetSocketAddress} of the master
   */
  @Override
  protected synchronized InetSocketAddress getAddress() {
    if (!mUseZookeeper) {
      return super.getAddress();
    }

    Preconditions.checkState(mTachyonConf.containsKey(Constants.ZOOKEEPER_ADDRESS));
    Preconditions.checkState(mTachyonConf.containsKey(Constants.ZOOKEEPER_LEADER_PATH));
    LeaderInquireClient leaderInquireClient =
        LeaderInquireClient.getClient(mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS),
            mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH));
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return NetworkAddressUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }
}
