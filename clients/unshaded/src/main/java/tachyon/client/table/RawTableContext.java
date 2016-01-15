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

import javax.annotation.concurrent.ThreadSafe;

import tachyon.client.ClientContext;

/**
 * A shared context in each client JVM for common raw table master client functionality such as a
 * pool of master clients. Any remote clients will be created and destroyed on a per use basis.
 */
@ThreadSafe
public enum RawTableContext {
  INSTANCE;

  /** Pool of raw table master clients. */
  private RawTableMasterClientPool mRawTableMasterClientPool;

  /**
   * Creates a new Raw Table Context.
   */
  RawTableContext() {
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }

  /**
   * Acquires a Raw Table master client from the pool. If all clients are being utilized, this
   * call will block until one is free. The client must be released by the caller after they have
   * finished using it.
   *
   * @return a raw table master client
   */
  public RawTableMasterClient acquireMasterClient() {
    return mRawTableMasterClientPool.acquire();
  }

  /**
   * Returns a Raw Table master client to the client pool. The client should not be accessed by
   * the caller after this method.
   *
   * @param masterClient the raw table master client to return
   */
  public void releaseMasterClient(RawTableMasterClient masterClient) {
    mRawTableMasterClientPool.release(masterClient);
  }

  /**
   * Re-initializes the raw table context. This should only be called by {@link ClientContext}
   */
  // TODO(calvin): Rethink resetting contexts outside of test cases
  public void reset() {
    mRawTableMasterClientPool.close();
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }
}
