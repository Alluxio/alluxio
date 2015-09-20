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

package tachyon.worker.lineage;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import tachyon.worker.block.BlockDataManager;

/**
 * Responsible for managing the lineage storing into under file system.
 */
public final class LineageDataManager {
  /** Block data manager for access block info */
  private final BlockDataManager mBlockDataManager;
  private final List<Long> mPersistedFiles;

  public LineageDataManager(BlockDataManager blockDataManager) {
    mBlockDataManager = Preconditions.checkNotNull(blockDataManager);
    mPersistedFiles = Lists.newArrayList();
  }

  /**
   * Persists the blocks of a file into the under file system.
   *
   *@param fileId the id of the file.
   * @param blockIds the list of block ids.
   * @param filePath the destination path in the under file system.
   */
  public void persistFile(long fileId, List<Long> blockIds, String filePath) {
    // TODO persist

    synchronized (mPersistedFiles) {
      mPersistedFiles.add(fileId);
    }
  }

  public List<Long> fetchPersistedFiles() {
    List<Long> toReturn = Lists.newArrayList();
    synchronized (mPersistedFiles) {
      toReturn.addAll(mPersistedFiles);
      mPersistedFiles.clear();
      return toReturn;
    }
  }
}
