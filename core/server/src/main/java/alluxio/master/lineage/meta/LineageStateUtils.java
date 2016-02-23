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

package alluxio.master.lineage.meta;

import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for checking the state of lineage files.
 */
@ThreadSafe
public final class LineageStateUtils {
  private LineageStateUtils() {} // prevent instantiation

  /**
   * Checks if all the output files of the given lineage are completed.
   *
   * @param lineage the lineage to check
   * @param fileSystemMasterView the view of the file system master where the output file lies
   * @return true if all the output files of the given lineage are completed, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   */
  public static boolean isCompleted(Lineage lineage, FileSystemMasterView fileSystemMasterView)
      throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      FileInfo fileInfo = fileSystemMasterView.getFileInfo(outputFile);
      if (!fileInfo.isCompleted()) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param lineage the lineage to check
   * @param fileSystemMasterView the view of the file system master
   * @return true if the lineage needs recompute, false otherwise
   * @throws FileDoesNotExistException if any output file of the lineage does not exist
   */
  public static boolean needRecompute(Lineage lineage, FileSystemMasterView fileSystemMasterView)
      throws FileDoesNotExistException {
    List<Long> lostFiles = fileSystemMasterView.getLostFiles();
    for (long outputFile : lineage.getOutputFiles()) {
      if (lostFiles.contains(outputFile)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param lineage the lineage to check
   * @param fileSystemMasterView the view of the file system master
   * @return true if all the output files are persisted, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   */
  public static boolean isPersisted(Lineage lineage, FileSystemMasterView fileSystemMasterView)
      throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      if (fileSystemMasterView.getFilePersistenceState(outputFile) != PersistenceState.PERSISTED) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param lineage the lineage to check
   * @param fileSystemMasterView the view of the file system master
   * @return true if at least one of the output files is being persisted, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   */
  public static boolean isInCheckpointing(Lineage lineage,
      FileSystemMasterView fileSystemMasterView) throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      if (fileSystemMasterView
          .getFilePersistenceState(outputFile) == PersistenceState.IN_PROGRESS) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param lineage the lineage to check
   * @param fileSystemMasterView the view of the file system master
   * @return all the output files of the given lineage that are lost on the workers
   * @throws FileDoesNotExistException if any output file of the lineage does not exist
   */
  public static List<Long> getLostFiles(Lineage lineage, FileSystemMasterView fileSystemMasterView)
      throws FileDoesNotExistException {
    List<Long> result = Lists.newArrayList();
    List<Long> lostFiles = fileSystemMasterView.getLostFiles();
    for (long outputFile : lineage.getOutputFiles()) {
      if (lostFiles.contains(outputFile)) {
        result.add(outputFile);
      }
    }
    return result;
  }
}
