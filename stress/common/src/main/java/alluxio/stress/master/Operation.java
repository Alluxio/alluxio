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

/**
 * The operations for the master stress tests.
 */
public enum Operation {
  // Create files
  CREATE_FILE, // create fixed-N, create more in extra
  GET_BLOCK_LOCATIONS, // call for fixed-N
  GET_FILE_STATUS, // call for fixed-N
  LIST_DIR, // call for fixed-N
  LIST_DIR_LOCATED, // call for fixed-N
  OPEN_FILE, // open for fixed-N

  // Dependent on CreateFile
  RENAME_FILE, // rename fixed-N, then rename in extra, need plenty of extra
  DELETE_FILE, // delete fixed-N, then delete from extra, need plenty of extra

  // Create dirs
  CREATE_DIR, // create fixed-N, create more in extra
  ;
}
