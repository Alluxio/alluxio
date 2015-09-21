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

package tachyon.client.table;

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.util.io.PathUtils;

/**
 * The column of a <code>RawTable</code>.
 */
public class RawColumn {
  private final TachyonFS mTachyonFS;
  private final RawTable mRawTable;
  private final int mColumnIndex;

  /**
   * Creates a new <code>RawColumn</code>.
   *
   * @param tachyonClient the <code>TachyonFS</code> client
   * @param rawTable the <code>RawTable</code> table this column belongs to
   * @param columnIndex the column index
   */
  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    mTachyonFS = tachyonClient;
    mRawTable = rawTable;
    mColumnIndex = columnIndex;
  }

  /**
   * Creates a new column partition.
   *
   * TODO(hy): Creating file here should be based on id.
   *
   * @param pId the partition id
   * @return whether operation succeeded
   * @throws IOException when the partition the path is invalid or points to an existing object
   */
  public boolean createPartition(int pId) throws IOException {
    TachyonURI tUri =
        new TachyonURI(PathUtils.concatPath(mRawTable.getPath(),
            Constants.MASTER_COLUMN_FILE_PREFIX + mColumnIndex, pId));
    return mTachyonFS.createFile(tUri) > 0;
  }

  /**
   * Gets an existing partition.
   *
   * TODO(hy): Creating file here should be based on id.
   *
   * @param pId the partition id
   * @return <code>TachyonFile</code> with the given partition
   * @throws IOException when the partition the path is invalid or points to a non-existing object
   */
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  /**
   * Gets an existing partition.
   *
   * TODO(hy): Creating file here should be based on id.
   *
   * @param pId the partition id
   * @param cachedMetadata whether to use the file metadata cache
   * @return <code>TachyonFile</code> with the given partition
   * @throws IOException when the partition the path is invalid or points to a non-existing object
   */
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws IOException {
    TachyonURI tUri =
        new TachyonURI(PathUtils.concatPath(mRawTable.getPath(),
            Constants.MASTER_COLUMN_FILE_PREFIX + mColumnIndex, pId));
    return mTachyonFS.getFile(tUri, cachedMetadata);
  }

  /**
   * Identifies the number of column partitions.
   *
   * TODO(hy): Creating file here should be based on id.
   *
   * @return the number of column partitions
   * @throws IOException when any of the partition paths is invalid or points to a non-existing
   *         object
   */
  public int partitions() throws IOException {
    TachyonURI tUri =
        new TachyonURI(PathUtils.concatPath(mRawTable.getPath(),
            Constants.MASTER_COLUMN_FILE_PREFIX + mColumnIndex));
    return mTachyonFS.listStatus(tUri).size();
  }
}
