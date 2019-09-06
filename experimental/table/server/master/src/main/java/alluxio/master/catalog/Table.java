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

package alluxio.master.catalog;

import alluxio.table.common.udb.UdbTable;

import java.util.ArrayList;

/**
 * The table implementation which manages all the versions of the table.
 */
public class Table {
  private final String mName;
  private final UdbTable mUdbTable;
  private final ArrayList<TableVersion> mVersions;

  /**
   * Creates an instance.
   *
   * @param udbTable the udb table
   */
  public Table(UdbTable udbTable) {
    mUdbTable = udbTable;

    mName = mUdbTable.getName();
    mVersions = new ArrayList<>(2);
    TableVersion tableVersion = new TableVersion(mUdbTable.getSchema());
    tableVersion.addView(TableVersion.DEFAULT_VIEW_NAME, mUdbTable.getView());
    mVersions.add(tableVersion);
  }

  /**
   * @return the latest version of the table
   */
  public TableVersion get() {
    synchronized (mVersions) {
      return mVersions.get(mVersions.size() - 1);
    }
  }

  /**
   * @return the table name
   */
  public String getName() {
    return mName;
  }
}
