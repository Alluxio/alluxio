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

package tachyon.master.block;

import org.powermock.reflect.Whitebox;

import tachyon.collections.IndexedSet;
import tachyon.master.block.meta.MasterWorkerInfo;

/**
 * Class which provides access to private state of {@link BlockMaster}.
 */
public final class BlockMasterPrivateAccess {
  public static boolean isWorkerRegistered(BlockMaster master, long workerId) {
    IndexedSet<MasterWorkerInfo> workers = Whitebox.getInternalState(master, "mWorkers");
    IndexedSet.FieldIndex<MasterWorkerInfo> idIndex = Whitebox.getInternalState(master, "mIdIndex");
    if (workers == null || idIndex == null) {
      return false;
    }
    synchronized (workers) {
      MasterWorkerInfo workerInfo = workers.getFirstByField(idIndex, workerId);
      return workerInfo != null && workerInfo.isRegistered();
    }
  }
}
