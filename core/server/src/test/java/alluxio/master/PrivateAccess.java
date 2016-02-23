/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.master.block.BlockMaster;

import org.powermock.reflect.Whitebox;

/**
 * Class which provides access to private state of {@link AlluxioMaster}.
 */
public final class PrivateAccess {

  /**
   * Gets the {@link BlockMaster}.
   *
   * @param master the {@link AlluxioMaster}
   * @return the {@link BlockMaster}
   */
  public static BlockMaster getBlockMaster(AlluxioMaster master) {
    return Whitebox.getInternalState(master, "mBlockMaster");
  }
}
