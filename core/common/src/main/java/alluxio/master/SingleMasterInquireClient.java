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

package alluxio.master;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

/**
 * A {@link MasterInquireClient} which always returns a fixed master address.
 */
public class SingleMasterInquireClient implements MasterInquireClient {
  private final InetSocketAddress mAddress;

  /**
   * @param address the master address
   */
  public SingleMasterInquireClient(InetSocketAddress address) {
    mAddress = address;
  }

  @Override
  public InetSocketAddress getPrimaryRpcAddress() {
    return mAddress;
  }

  @Override
  public List<InetSocketAddress> getMasterRpcAddresses() {
    return Collections.singletonList(mAddress);
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
