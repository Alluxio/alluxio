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

package tachyon.master.lineage.recompute;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageStore;

/**
 * Plans the recompute strategy. It takes a list of lost files as input and outputs a recompute
 * plan.
 */
public class RecomputePlanner {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final LineageStore mLineageStore;
  private final FileSystemMaster mFileSystemMaster;

  public RecomputePlanner(LineageStore lineageStore, FileSystemMaster fileSystemMaster) {
    mLineageStore = Preconditions.checkNotNull(lineageStore);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
  }

  public RecomputePlan plan() {
    List<Long> lostFiles = mFileSystemMaster.getLostFiles();

    // lineage to recompute
    Set<Lineage> toRecompute = Sets.newHashSet();
    if(!lostFiles.isEmpty()) {
      LOG.info("report lost files " + lostFiles);
      // report lost files
      for (long lostFile : lostFiles) {
        Lineage lineage = mLineageStore.reportLostFile(lostFile);
        if (!lineage.isPersisted()) {
          toRecompute.add(lineage);
        }
      }
    }

    List<Lineage> toRecomputeAfterSort = mLineageStore.sortLineageTopologically(toRecompute);
    RecomputePlan plan = new RecomputePlan(toRecomputeAfterSort);
    return plan;
  }
}
