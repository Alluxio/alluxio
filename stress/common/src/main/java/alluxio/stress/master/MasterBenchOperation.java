/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.stress.master;

import alluxio.stress.Operation;

/**
 * The operations for the master stress tests.
 */
public enum MasterBenchOperation implements Operation<MasterBenchOperation> {
  // Create files
  CREATE_FILE("CreateFile"), // create fixed-N, create more in extra
  GET_BLOCK_LOCATIONS("GetBlockLocations"), // call for fixed-N
  GET_FILE_STATUS("GetFileStatus"), // call for fixed-N
  LIST_DIR("ListDir"), // call for fixed-N
  LIST_DIR_LOCATED("ListDirLocated"), // call for fixed-N
  OPEN_FILE("OpenFile"), // open for fixed-N

  // Dependent on CreateFile
  RENAME_FILE("RenameFile"), // rename fixed-N, then rename in extra, need plenty of extra
  DELETE_FILE("DeleteFile"), // delete fixed-N, then delete from extra, need plenty of extra

  // Create dirs
  CREATE_DIR("CreateDir"), // create fixed-N, create more in extra
  ;

  private final String mName;

  /**
   * @param name Name of the operation
   */
  MasterBenchOperation(String name) {
    mName = name;
  }

  @Override
  public String toString() {
    return mName;
  }

  @Override
  public MasterBenchOperation[] enumValues() {
    return MasterBenchOperation.values();
  }
}
