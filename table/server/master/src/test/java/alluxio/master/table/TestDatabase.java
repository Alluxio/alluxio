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

package alluxio.master.table;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A udb implementation which does nothing, used for testing.
 */
public class TestDatabase implements UnderDatabase {
  public static final String DB_NAME = "test_udb_name";
  public static final String TABLE_NAME_PREFIX = "test_table_name";
  private static final TestDatabase DATABASE = new TestDatabase();

  private Map<String, UdbTable> mUdbTables;

  private TestDatabase() {
    mUdbTables = new HashMap<>();
  }

  /**
   * Creates an instance.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static TestDatabase create(UdbContext udbContext,
      UdbConfiguration configuration) {
    return DATABASE;
  }

  @Override
  public String getType() {
    return TestUdbFactory.TYPE;
  }

  @Override
  public String getName() {
    return DB_NAME;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    return new ArrayList<>(mUdbTables.keySet());
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    return mUdbTables.get(tableName);
  }

  public static String getTableName(int i) {
    return TABLE_NAME_PREFIX + Integer.toString(i);
  }

  public static void genTable(int numOfTable, int numOfPartitions) {
    DATABASE.mUdbTables.clear();
    for (int i = 0; i < numOfTable; i++) {
      DATABASE.mUdbTables.put(getTableName(i),
          new TestUdbTable(DB_NAME, getTableName(i), numOfPartitions));
    }
  }
}
