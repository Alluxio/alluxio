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

package alluxio.master.file;

import alluxio.proto.journal.File;

/**
 * Complete File Result containing two update entries to the journal.
 */
public class CompleteFileResult {
  private File.UpdateInodeEntry mUpdateInodeEntry;
  private File.UpdateInodeFileEntry mUpdateInodeFileEntry;

  /**
   * Construct a complete file result object.
   */
  CompleteFileResult(File.UpdateInodeEntry updateInodeEntry,
      File.UpdateInodeFileEntry updateInodeFileEntry) {
    mUpdateInodeEntry = updateInodeEntry;
    mUpdateInodeFileEntry = updateInodeFileEntry;
  }

  /**
   * Get the updateInodeEntry object.
   *
   * @return the inode update object
   */
  public File.UpdateInodeEntry getUpdateInodeEntry() {
    return mUpdateInodeEntry;
  }

  /**
   * Get the update inode file object.
   *
   * @return the inode file update object
   */
  public File.UpdateInodeFileEntry getUpdateInodeFileEntry() {
    return mUpdateInodeFileEntry;
  }
}
