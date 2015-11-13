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

package tachyon.client.table;

import tachyon.client.ClientContext;

/**
 * A shared context in each client JVM for common raw table functionality.
 */
public enum RawTableContext {
  INSTANCE;

  private RawTableMasterClientPool mRawTableMasterClientPool;

  /**
   * Creates a new lineage context.
   */
  RawTableContext() {
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }

  /**
   * Acquires a lineage master client from the lineage master client pool.
   *
   * @return the acquired lineage master client
   */
  public RawTableMasterClient acquireMasterClient() {
    return mRawTableMasterClientPool.acquire();
  }

  /**
   * Releases a lineage master client into the lineage master client pool.
   *
   * @param masterClient a lineage master client to release
   */
  public void releaseMasterClient(RawTableMasterClient masterClient) {
    mRawTableMasterClientPool.release(masterClient);
  }
}
