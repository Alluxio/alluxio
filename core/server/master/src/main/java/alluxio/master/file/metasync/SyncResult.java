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

package alluxio.master.file.metasync;

/**
 * Metadata sync results.
 */
public class SyncResult {
  /**
   * Constructs a sync result object.
   * @param success if the sync succeeded
   * @param numSyncedFiles the number of files synced
   */
  public SyncResult(boolean success, long numSyncedFiles) {
    mSuccess = success;
    mNumSyncedFiles = numSyncedFiles;
  }

  private final boolean mSuccess;
  private final long mNumSyncedFiles;

  /**
   * @return if the metadata sync succeeded
   */
  public boolean getSuccess() {
    return mSuccess;
  }

  /**
   * @return the number of synced files
   */
  public long getNumSyncedFiles() {
    return mNumSyncedFiles;
  }
}
