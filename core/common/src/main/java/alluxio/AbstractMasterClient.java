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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class AbstractMasterClient extends AbstractClient {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyKey.LOGGER_TYPE);

  /**
   * Identifies whether the ZooKeeper service should be used for obtaining master address.
   */
  protected final boolean mUseZookeeper;

  /**
   * Creates a new master client base.
   *
   * @param masterAddress the master address
   */
  public AbstractMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress, "master");
    mUseZookeeper = Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  /**
   * Returns the {@link InetSocketAddress} of the master. If zookeeper is used, this will consult
   * the zookeeper instance for the master address.
   *
   * @return the {@link InetSocketAddress} of the master
   */
  @Override
  public synchronized InetSocketAddress getAddress() {
    if (!mUseZookeeper) {
      return super.getAddress();
    }
    return NetworkAddressUtils.getMasterAddressFromZK();
  }
}
