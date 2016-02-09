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

package alluxio;

import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class MasterClientBase extends AbstractClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Identifies whether the ZooKeeper service should be used for obtaining master address.
   */
  protected final boolean mUseZookeeper;

  /**
   * Creates a new master client base.
   *
   * @param masterAddress the master address
   * @param configuration the Alluxio configuration
   */
  public MasterClientBase(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration, "master");
    mUseZookeeper = mConfiguration.getBoolean(Constants.ZOOKEEPER_ENABLED);
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

    Preconditions.checkState(mConfiguration.containsKey(Constants.ZOOKEEPER_ADDRESS));
    Preconditions.checkState(mConfiguration.containsKey(Constants.ZOOKEEPER_LEADER_PATH));
    LeaderInquireClient leaderInquireClient =
        LeaderInquireClient.getClient(mConfiguration.get(Constants.ZOOKEEPER_ADDRESS),
            mConfiguration.get(Constants.ZOOKEEPER_LEADER_PATH), mConfiguration);
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return NetworkAddressUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }
}
