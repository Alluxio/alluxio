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

package alluxio.master.lineage.recompute;

import alluxio.Constants;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.lineage.meta.Lineage;
import alluxio.master.lineage.meta.LineageStateUtils;
import alluxio.master.lineage.meta.LineageStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Plans the recompute strategy. It takes a list of lost files as input and outputs a recompute
 * plan.
 */
@ThreadSafe
public class RecomputePlanner {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final LineageStore mLineageStore;
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link RecomputePlanner}.
   *
   * @param lineageStore the lineage store
   * @param fileSystemMaster the file system master
   */
  public RecomputePlanner(LineageStore lineageStore, FileSystemMaster fileSystemMaster) {
    mLineageStore = Preconditions.checkNotNull(lineageStore);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
  }

  /**
   * @return a {@link RecomputePlan} that identifies the lineages to recompute
   */
  public RecomputePlan plan() {
    List<Long> lostFiles = mFileSystemMaster.getLostFiles();

    // lineage to recompute
    Set<Lineage> toRecompute = Sets.newHashSet();
    if (!lostFiles.isEmpty()) {
      LOG.info("report lost files {}", lostFiles);
      // report lost files
      for (long lostFile : lostFiles) {
        if (!mLineageStore.hasOutputFile(lostFile)) {
          continue;
        }

        Lineage lineage;
        try {
          lineage = mLineageStore.getLineageOfOutputFile(lostFile);
        } catch (LineageDoesNotExistException e) {
          throw new IllegalStateException(e); // should not happen
        }
        try {
          if (!LineageStateUtils.isPersisted(lineage,
              mFileSystemMaster.getFileSystemMasterView())) {
            toRecompute.add(lineage);
          }
        } catch (FileDoesNotExistException e) {
          throw new IllegalStateException(e); // should not happen
        }
      }
    }

    List<Lineage> toRecomputeAfterSort = mLineageStore.sortLineageTopologically(toRecompute);
    return new RecomputePlan(toRecomputeAfterSort);
  }
}
