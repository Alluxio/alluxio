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

package alluxio.multi.process;

import alluxio.client.meta.MetaMasterClient;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;

/**
 * Container for various Alluxio clients.
 */
public final class Clients {
  private final FileSystem mFs;
  private final FileSystemMasterClient mFsMaster;
  private final MetaMasterClient mMetaMaster;
  private final BlockMasterClient mBlockMaster;

  /**
   * @param fs filesystem client
   * @param fsMaster filesystem master client
   * @param metaMaster meta master client
   * @param blockMaster block master client
   */
  public Clients(FileSystem fs, FileSystemMasterClient fsMaster, MetaMasterClient metaMaster,
      BlockMasterClient blockMaster) {
    mFs = fs;
    mFsMaster = fsMaster;
    mMetaMaster = metaMaster;
    mBlockMaster = blockMaster;
  }

  /**
   * @return the filesystem client
   */
  public FileSystem getFileSystemClient() {
    return mFs;
  }

  /**
   * @return the filesystem master client
   */
  public FileSystemMasterClient getFileSystemMasterClient() {
    return mFsMaster;
  }

  /**
   * @return the meta master client
   */
  public MetaMasterClient getMetaMasterClient() {
    return mMetaMaster;
  }

  /**
   * @return the block master client
   */
  public BlockMasterClient getBlockMasterClient() {
    return mBlockMaster;
  }
}
