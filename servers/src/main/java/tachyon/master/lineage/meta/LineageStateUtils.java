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

package tachyon.master.lineage.meta;

import java.util.List;

import com.google.common.collect.Lists;

import tachyon.exception.FileDoesNotExistException;
import tachyon.master.file.meta.FilePersistenceState;
import tachyon.master.file.meta.FileStoreView;
import tachyon.thrift.FileInfo;

/**
 * Utility methods for checking the state of lineage files.
 */
public final class LineageStateUtils {
  private LineageStateUtils() {} // prevent instantiation

  /**
   * Checks if all the output files of the given lineage are completed.
   *
   * @return true if all the output files of the given lineage are completed, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   */
  public static boolean isCompleted(Lineage lineage, FileStoreView fileStoreView)
      throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      FileInfo fileInfo = fileStoreView.getFileInfo(outputFile);
      if (!fileInfo.isCompleted) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if the lineage needs recompute, false otherwise
   * @throws FileDoesNotExistException if any output file of the lineage does not exist
   */
  public static boolean needRecompute(Lineage lineage, FileStoreView fileStoreView)
      throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      FileInfo fileInfo = fileStoreView.getFileInfo(outputFile);
      if (fileInfo.isLost) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if all the output files are persisted, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   */
  public static boolean isPersisted(Lineage lineage, FileStoreView fileStoreView)
      throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      if (fileStoreView.getFilePersistenceState(outputFile) != FilePersistenceState.PERSISTED) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if at least one of the output files is being persisted, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   */
  public static boolean isInCheckpointing(Lineage lineage, FileStoreView fileStoreView)
      throws FileDoesNotExistException {
    for (long outputFile : lineage.getOutputFiles()) {
      if (fileStoreView.getFilePersistenceState(outputFile) == FilePersistenceState.SCHEDULED
          || fileStoreView.getFilePersistenceState(outputFile) == FilePersistenceState.PERSISTING) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return all the output files of the given lineage that are lost on the workers
   * @throws FileDoesNotExistException if any output file of the lineage does not exist
   */
  public static List<Long> getLostFiles(Lineage lineage, FileStoreView fileStoreView)
      throws FileDoesNotExistException {
    List<Long> result = Lists.newArrayList();
    for (long outputFile : lineage.getOutputFiles()) {
      FileInfo fileInfo = fileStoreView.getFileInfo(outputFile);
      if (fileInfo.isLost) {
        result.add(outputFile);
      }
    }
    return result;
  }
}
