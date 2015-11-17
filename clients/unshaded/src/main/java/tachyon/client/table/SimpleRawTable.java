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
 * Tachyon provides native support for tables with multiple columns. Each table contains one or more
 * columns. Each column contains one or more ordered files.
 */
@PublicApi
public class SimpleRawTable {

  /** Id of the raw table, which uniquely identifies this table */
  private final long mRawTableId;

  /**
   * Creates a raw table which is used as a handler for accessing raw tables in
   * {@link TachyonRawTables}
   *
   * @param rawTableId the id of the raw table
   */
  public SimpleRawTable(long rawTableId) {
    mRawTableId = rawTableId;
  }

  /**
   * @return the id of the raw table
   */
  public long getRawTableId() {
    return mRawTableId;
  }
}
