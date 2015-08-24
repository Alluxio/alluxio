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

package tachyon.client.next.file;

import tachyon.client.next.ClientContext;
import tachyon.client.next.block.BlockMasterClientPool;
import tachyon.client.next.block.TachyonBS;
import tachyon.master.MasterClient;

/**
 * A shared context in each client JVM for common File System client functionality such as a pool
 * of master clients.
 */
public enum FSContext {
  INSTANCE;

  // TODO: Separate this when block master and file system master use different clients
  private final BlockMasterClientPool mFileSystemMasterClientPool;
  private final TachyonBS mTachyonBS;

  private FSContext() {
    mFileSystemMasterClientPool =
        new BlockMasterClientPool(ClientContext.getMasterAddress(), ClientContext.getConf());
    mTachyonBS = TachyonBS.get();
  }

  public MasterClient acquireMasterClient() {
    return mFileSystemMasterClientPool.acquire();
  }

  public void releaseMasterClient(MasterClient masterClient) {
    mFileSystemMasterClientPool.release(masterClient);
  }

  public TachyonBS getTachyonBS() {
    return mTachyonBS;
  }
}
