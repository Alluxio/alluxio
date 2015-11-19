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

import tachyon.annotation.PublicApi;

/**
 * The column of a {@link RawTable}. A RawColumn is uniquely identified by the raw table it
 * belongs to and its column index. Instances of this class should be used as arguments to the
 * column specific commands in {@link TachyonRawTables}. An instance of this class can be
 * obtained through {@link RawTable#getColumn}.
 */
@PublicApi
public final class RawColumn {
  private final RawTable mRawTable;
  private final int mColumnIndex;

  /**
   * Creates a new {@code RawColumn}.
   *
   * @param rawTable the {@code RawTable} table this column belongs to
   * @param columnIndex the column index
   */
  RawColumn(RawTable rawTable, int columnIndex) {
    mRawTable = rawTable;
    mColumnIndex = columnIndex;
  }

  /**
   * @return the index of the column within its raw table, the first column of a raw table has
   * column index 0
   */
  public int getColumnIndex() {
    return mColumnIndex;
  }

  /**
   * @return the raw table which contains this column
   */
  public RawTable getRawTable() {
    return mRawTable;
  }
}
