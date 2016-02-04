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

package alluxio.master;

import org.powermock.reflect.Whitebox;

import alluxio.master.block.BlockMaster;

/**
 * Class which provides access to private state of {@link TachyonMaster}.
 */
public final class TachyonMasterPrivateAccess {

  /**
   * Gets the {@link BlockMaster}.
   *
   * @param master the {@link TachyonMaster}
   * @return the {@link BlockMaster}
   */
  public static BlockMaster getBlockMaster(TachyonMaster master) {
    return Whitebox.getInternalState(master, "mBlockMaster");
  }
}
