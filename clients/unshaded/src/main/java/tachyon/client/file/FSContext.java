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

package tachyon.client.file;

import tachyon.client.FileSystemMasterClient;
import tachyon.client.ClientContext;
import tachyon.client.block.TachyonBlockStore;

/**
 * A shared context in each client JVM for common File System client functionality such as a pool
 * of master clients.
 */
public enum FSContext {
  INSTANCE;

  private FSMasterClientPool mFileSystemMasterClientPool;
  private final TachyonBlockStore mTachyonBlockStore;

  private FSContext() {
    mFileSystemMasterClientPool =
        new FSMasterClientPool(ClientContext.getMasterAddress(), ClientContext.getConf());
    mTachyonBlockStore = TachyonBlockStore.get();
  }

  /**
   * Reinitializes the File System Context. This method should only be used in ClientContext.
   */
  public void resetContext() {
    mFileSystemMasterClientPool.close();
    mFileSystemMasterClientPool =
        new FSMasterClientPool(ClientContext.getMasterAddress(), ClientContext.getConf());
  }

  public FileSystemMasterClient acquireMasterClient() {
    return mFileSystemMasterClientPool.acquire();
  }

  public void releaseMasterClient(FileSystemMasterClient masterClient) {
    mFileSystemMasterClientPool.release(masterClient);
  }

  public TachyonBlockStore getTachyonBS() {
    return mTachyonBlockStore;
  }
}
