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

package tachyon.master.lineage.checkpoint;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.master.file.FileSystemMaster;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.meta.LineageStoreView;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;

/**
 * Manages checkpointing. This class is thread-safe
 *
 * TODO(yupeng): relax the locking
 */
public final class CheckpointManager {
  private final LineageStore mLineageStore;
  private final FileSystemMaster mFileSystemMaster;
  private Map<Long, List<CheckpointFile>> mWorkerToCheckpointFile;

  public CheckpointManager(LineageStore lineageStore, FileSystemMaster fileSystemMaster) {
    mLineageStore = Preconditions.checkNotNull(lineageStore);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mWorkerToCheckpointFile = Maps.newHashMap();
  }

  public LineageStoreView getLineageStoreView() {
    return new LineageStoreView(mLineageStore);
  }

  public void acceptPlan(CheckpointPlan plan) {
    for (Lineage lineage : plan.getLineagesToCheckpoint()) {
      // register the lineage file to checkpoint
      for (LineageFile file : lineage.getOutputFiles()) {
        // find the worker
        long workerId = findStoringWorker(file);
        if (!mWorkerToCheckpointFile.containsKey(workerId)) {
          mWorkerToCheckpointFile.put(workerId, Lists.<CheckpointFile>newArrayList());
        }
        mWorkerToCheckpointFile.get(workerId).add(new CheckpointFile(file));
      }
    }
  }

  /**
   * Finds the files to send to the given worker for checkpoint
   *
   * @param workerId the worker id
   * @return the list of files.
   */
  public List<Long> getFilesToCheckpoint(long workerId) {
    List<Long> fileIds = Lists.newArrayList();
    if (!mWorkerToCheckpointFile.containsKey(workerId)) {
      return fileIds;
    }

    for (CheckpointFile file : mWorkerToCheckpointFile.get(workerId)) {
      if (!file.mSentToWorker) {
        file.mSentToWorker = true;
        fileIds.add(file.mFile.getFileId());
      }
    }

    return fileIds;
  }

  private long findStoringWorker(LineageFile file) {
    List<Long> workers = Lists.newArrayList();
    try {
      for (FileBlockInfo fileBlockInfo : mFileSystemMaster.getFileBlockInfoList(file.getFileId())) {
        for (BlockLocation blockLocation : fileBlockInfo.blockInfo.locations) {
          workers.add(blockLocation.workerId);
        }
      }
    } catch (FileDoesNotExistException e) {
      // should not happen
      throw new RuntimeException(e);
    }

    Preconditions.checkState(workers.size() == 1, "the file is stored at more than one worker");
    return workers.get(0);
  }

  class CheckpointFile {
    LineageFile mFile;
    boolean mSentToWorker;

    public CheckpointFile(LineageFile file) {
      mFile = Preconditions.checkNotNull(file);
    }
  }
}
