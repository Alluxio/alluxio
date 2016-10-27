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

package alluxio;

import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class AbstractMasterClient extends AbstractClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Identifies the Zookeeper path to use for discovering the master address. This should be null
   * if Zookeeper is not being used.
   */
  protected final String mZkLeaderPath;

  /**
   * Creates a new master client base.
   *
   * @param masterAddress the master address
   */
  public AbstractMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress, "master");
    if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      mZkLeaderPath = Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH);
    } else {
      mZkLeaderPath = null;
    }
  }

  /**
   * Creates a new master client base.
   *
   * @param zkLeaderPath the Zookeeper path holding the leader master address
   */
  public AbstractMasterClient(String zkLeaderPath) {
    super(getAddress(zkLeaderPath), "master");
    Preconditions.checkState(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    mZkLeaderPath = zkLeaderPath;
  }

  /**
   * @return the {@link InetSocketAddress} of the master
   */
  @Override
  public synchronized InetSocketAddress getAddress() {
    if (mZkLeaderPath == null) {
      return super.getAddress();
    }
    return getAddress(mZkLeaderPath);
  }

  private static InetSocketAddress getAddress(String zkLeaderPath) {
    return NetworkAddressUtils.getMasterAddressFromZK(zkLeaderPath);
  }
}
