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

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.resource.ResourcePool;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A fixed pool of FileSystemMasterClient instances.
 */
@ThreadSafe
public final class FileSystemMasterClientPool extends ResourcePool<FileSystemMasterClient> {
  private final InetSocketAddress mMasterAddress;

  /**
   * Creates a new file system master client pool.
   *
   * @param masterAddress the master address
   */
  public FileSystemMasterClientPool(InetSocketAddress masterAddress) {
    super(Configuration.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS));
    mMasterAddress = masterAddress;
  }

  /**
   * Creates a new file system master client pool.
   *
   * @param masterAddress the master address
   * @param clientThreads the number of client threads to use
   */
  public FileSystemMasterClientPool(InetSocketAddress masterAddress, int clientThreads) {
    super(clientThreads);
    mMasterAddress = masterAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down
  }

  @Override
  protected FileSystemMasterClient createNewResource() {
    return new FileSystemMasterClient(mMasterAddress);
  }
}
