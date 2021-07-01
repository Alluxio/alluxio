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

import alluxio.client.file.FileSystem;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.PrincipalType;
import alluxio.table.common.udb.UdbBypassSpec;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A udb implementation which does nothing, used for testing.
 */
public class TestDatabase implements UnderDatabase {
  public static final String TEST_UDB_NAME = "test_udb_name";
  public static final String TABLE_NAME_PREFIX = "test_table_name";
  public static DatabaseInfo sTestDbInfo = new DatabaseInfo("test://test", "TestOwner",
      PrincipalType.USER, "comment", ImmutableMap.of("testkey", "testvalue"));

  private static final TestDatabase DATABASE = new TestDatabase();

  private Map<String, UdbTable> mUdbTables;
  private UdbContext mUdbContext;
  /**
   * The names of the threads calling getTable(). This is useful for examining parallel sync
   * behavior.
   */
  private Set<String> mGetTableThreadNames = new ConcurrentHashSet<>();

  private TestDatabase() {
    mUdbTables = new HashMap<>();
  }

  /**
   * Resets the db by clearing out all tables.
   */
  public static void reset() {
    DATABASE.mUdbTables.clear();
    resetGetTableThreadNames();
  }

  /**
   * Resets the set of thread names for getTable.
   */
  public static void resetGetTableThreadNames() {
    DATABASE.mGetTableThreadNames.clear();
  }

  /**
   * @return the set of thread names used to call getTable
   */
  public static Set<String> getTableThreadNames() {
    return DATABASE.mGetTableThreadNames;
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
    DATABASE.setUdbContext(udbContext);
    return DATABASE;
  }

  private void checkDbName() throws NotFoundException {
    if (!getUdbContext().getUdbDbName().equals(TEST_UDB_NAME)) {
      throw new NotFoundException("Database " + getUdbContext().getDbName() + " does not exist.");
    }
  }

  @Override
  public String getType() {
    return TestUdbFactory.TYPE;
  }

  @Override
  public String getName() {
    return TEST_UDB_NAME;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    checkDbName();
    return new ArrayList<>(mUdbTables.keySet());
  }

  @Override
  public UdbTable getTable(String tableName, UdbBypassSpec bypassSpec) throws IOException {
    checkDbName();
    if (!mUdbTables.containsKey(tableName)) {
      throw new NotFoundException("Table " + tableName + " does not exist.");
    }
    mGetTableThreadNames.add(Thread.currentThread().getName());
    return mUdbTables.get(tableName);
  }

  public static String getTableName(int i) {
    return TABLE_NAME_PREFIX + Integer.toString(i);
  }

  public static void genTable(int numOfTable, int numOfPartitions, boolean generateFiles) {
    DATABASE.mUdbTables.clear();
    FileSystem fs = null;
    if (generateFiles) {
      fs = FileSystem.Factory.create(ServerConfiguration.global());
    }
    for (int i = 0; i < numOfTable; i++) {
      DATABASE.mUdbTables.put(getTableName(i),
          new TestUdbTable(TEST_UDB_NAME, getTableName(i), numOfPartitions, fs));
    }
  }

  private void setUdbContext(UdbContext udbContext) {
    mUdbContext = udbContext;
  }

  @Override
  public UdbContext getUdbContext() {
    return mUdbContext;
  }

  @Override
  public DatabaseInfo getDatabaseInfo() {
    return sTestDbInfo;
  }
}
