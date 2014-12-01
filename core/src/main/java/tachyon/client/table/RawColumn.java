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

import tachyon.TachyonURI;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.master.MasterInfo;
import tachyon.util.CommonUtils;

/**
 * The column of a <code>RawTable</code>.
 */
public class RawColumn {
  private final TachyonFS mTachyonFS;
  private final RawTable mRawTable;
  private final int mColumnIndex;

  /**
   * @param tachyonClient
   * @param rawTable
   * @param columnIndex
   */
  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    mTachyonFS = tachyonClient;
    mRawTable = rawTable;
    mColumnIndex = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws IOException {
    TachyonURI tUri =
        new TachyonURI(CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex, pId));
    return mTachyonFS.createFile(tUri) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws IOException {
    TachyonURI tUri =
        new TachyonURI(CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex, pId));
    return mTachyonFS.getFile(tUri, cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws IOException {
    TachyonURI tUri =
        new TachyonURI(CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex));
    return mTachyonFS.listStatus(tUri).size();
  }
}
