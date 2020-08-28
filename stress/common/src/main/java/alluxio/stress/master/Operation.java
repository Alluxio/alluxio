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
  CreateFile, // create fixed-N, create more in extra
  GetBlockLocations, // call for fixed-N
  GetFileStatus, // call for fixed-N
  ListDir, // call for fixed-N
  ListDirLocated, // call for fixed-N
  OpenFile, // open for fixed-N

  // Dependent on CreateFile
  RenameFile, // rename fixed-N, then rename in extra, need plenty of extra
  DeleteFile, // delete fixed-N, then delete from extra, need plenty of extra

  // Create dirs
  CreateDir, // create fixed-N, create more in extra
  ;
}
