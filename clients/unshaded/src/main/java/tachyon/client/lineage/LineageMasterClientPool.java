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

package tachyon.client.lineage;

import java.net.InetSocketAddress;

import tachyon.client.ClientContext;
import tachyon.resource.ResourcePool;

public class LineageMasterClientPool extends ResourcePool<LineageMasterClient> {
  private static final int CAPACITY = 10;
  private final InetSocketAddress mMasterAddress;

  /**
   * Creates a new lineage master client pool.
   *
   * @param masterAddress the master address
   */
  public LineageMasterClientPool(InetSocketAddress masterAddress) {
    // TODO: Get capacity from configuration.
    super(CAPACITY);
    mMasterAddress = masterAddress;
  }

  @Override
  public void close() {
    // TODO: Consider collecting all the clients and shutting them down
  }

  @Override
  protected LineageMasterClient createNewResource() {
    return new LineageMasterClient(mMasterAddress, ClientContext.getConf());
  }
}
