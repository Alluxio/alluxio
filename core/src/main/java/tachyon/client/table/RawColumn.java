/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client.table;

import java.io.IOException;

import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.master.MasterInfo;
import tachyon.util.CommonUtils;

/**
 * The column of a <code>RawTable</code>.
 */
public class RawColumn {
  private final TachyonFS TFS;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  /**
   * @param tachyonClient
   * @param rawTable
   * @param columnIndex
   */
  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    TFS = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws IOException {
    return TFS.createFile(CommonUtils.concat(RAW_TABLE.getPath(), MasterInfo.COL + COLUMN_INDEX,
        pId)) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws IOException {
    return TFS.getFile(
        CommonUtils.concat(RAW_TABLE.getPath(), MasterInfo.COL + COLUMN_INDEX, pId),
        cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws IOException {
    return TFS.getNumberOfFiles(CommonUtils.concat(RAW_TABLE.getPath(), MasterInfo.COL
        + COLUMN_INDEX));
  }
}
