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

package alluxio.master.block;

import alluxio.collections.IndexedSet;
import alluxio.master.block.meta.MasterWorkerInfo;

import org.powermock.reflect.Whitebox;

/**
 * Class which provides access to private state of {@link BlockMaster}.
 */
public final class BlockMasterPrivateAccess {
  /**
   * Checks whether a worker with the given workerId is registered with the given block master.
   *
   * @param master the block master
   * @param workerId the workerId
   * @return true if the worker has registered, false otherwise
   */
  public static boolean isWorkerRegistered(BlockMaster master, long workerId) {
    IndexedSet<MasterWorkerInfo> workers = Whitebox.getInternalState(master, "mWorkers");
    IndexedSet.FieldIndex<MasterWorkerInfo> idIndex = Whitebox.getInternalState(master, "mIdIndex");
    synchronized (workers) {
      MasterWorkerInfo workerInfo = workers.getFirstByField(idIndex, workerId);
      return workerInfo != null && workerInfo.isRegistered();
    }
  }
}
