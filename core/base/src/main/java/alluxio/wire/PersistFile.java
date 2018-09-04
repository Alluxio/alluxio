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

package alluxio.wire;

import java.util.List;

/**
 * Class to represent a file persist info.
 */
public final class PersistFile {
  private List<Long> mBlockIds;
  private long mFileId;

  /**
   * Creates a new instance for persist file.
   *
   * @param fileId the file id
   * @param blockIds the list of block ids to persist
   */
  public PersistFile(long fileId, List<Long> blockIds) {
    mFileId = fileId;
    mBlockIds = blockIds;
  }

  public List<Long> getBlockIds() {
    return mBlockIds;
  }

  public long getFileId() {
    return mFileId;
  }

  public void setBlockIds(List<Long> blockIds) {
    this.mBlockIds = blockIds;
  }

  public void setFileId(long fileId) {
    this.mFileId = fileId;
  }
}
