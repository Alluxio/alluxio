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

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.FileSystemMasterClient;
import tachyon.resource.ResourcePool;
import tachyon.util.ThreadFactoryUtils;

final class FileSystemMasterClientPool extends ResourcePool<FileSystemMasterClient> {
  private final ExecutorService mExecutorService;
  private final InetSocketAddress mMasterAddress;

  /**
   * Creates a new file stream master client pool.
   *
   * @param masterAddress the master address
   */
  public FileSystemMasterClientPool(InetSocketAddress masterAddress) {
    super(ClientContext.getConf().getInt(Constants.USER_FILE_MASTER_CLIENT_THREADS));
    mExecutorService = Executors.newFixedThreadPool(mMaxCapacity,
        ThreadFactoryUtils.build("fs-master-heartbeat-%d", true));
    mMasterAddress = masterAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down
    mExecutorService.shutdown();
  }

  @Override
  protected FileSystemMasterClient createNewResource() {
    return new FileSystemMasterClient(mMasterAddress, mExecutorService, ClientContext.getConf());
  }
}
