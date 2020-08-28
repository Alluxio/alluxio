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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The file block with location information.
 */
@PublicApi
@NotThreadSafe
public final class BlockLocationInfo {
  private final FileBlockInfo mBlockInfo;
  private final List<WorkerNetAddress> mLocations;

  /**
   * @param blockInfo the block info
   * @param locations the addresses of the workers containing the block
   */
  public BlockLocationInfo(FileBlockInfo blockInfo, List<WorkerNetAddress> locations) {
    mBlockInfo = blockInfo;
    mLocations = locations;
  }

  /**
   * @return the block info
   */
  public FileBlockInfo getBlockInfo() {
    return mBlockInfo;
  }

  /**
   * @return the addresses of the workers containing the block
   */
  public List<WorkerNetAddress> getLocations() {
    return mLocations;
  }
}
