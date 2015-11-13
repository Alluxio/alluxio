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
import tachyon.client.RawTableMasterClient;

public enum RawTablesContext {
  INSTANCE;

  private RawTableMasterClientPool mRawTableMasterClientPool;

  RawTablesContext() {
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }

  public RawTableMasterClient acquireMasterClient() {
    return mRawTableMasterClientPool.acquire();
  }

  public void releaseMasterClient(RawTableMasterClient masterClient) {
    mRawTableMasterClientPool.release(masterClient);
  }

  // TODO(calvin): Rethink resetting contexts outside of test cases
  public void reset() {
    mRawTableMasterClientPool.close();
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }
}
