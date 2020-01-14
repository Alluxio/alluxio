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

package alluxio.server.ft.journal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.master.LocalAlluxioCluster;
import alluxio.master.table.Table;
import alluxio.master.table.TableMaster;
import alluxio.master.table.TestDatabase;
import alluxio.master.table.TestUdbFactory;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for table master functionality.
 */
public class TableMasterJournalIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false)
          .build();

  private static final String DB_NAME = TestDatabase.TEST_UDB_NAME;

  @Before
  public void before() throws Exception {
    TestDatabase.genTable(1, 2);
  }

  @Test
  public void journalSync() throws Exception {
    mClusterResource.start();
    LocalAlluxioCluster mCluster = mClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);

    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap());
    checkTable(tableMaster, DB_NAME, 1, 2);

    // Update Udb, the table should stay the same, until we sync
    TestDatabase.genTable(2, 3);
    checkTable(tableMaster, DB_NAME, 1, 2);
    assertEquals(TestDatabase.getTableName(0), tableMaster.getAllTables(DB_NAME).get(0));
    assertEquals(1, tableMaster.getAllTables(DB_NAME).size());
    assertEquals(2, tableMaster.getTable(DB_NAME, TestDatabase.getTableName(0))
        .getPartitions().size());

    tableMaster.syncDatabase(DB_NAME);
    checkTable(tableMaster, DB_NAME, 2, 3);

    mCluster.stopMasters();
    mCluster.startMasters();

    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    tableMasterRestart.syncDatabase(DB_NAME);
    checkTable(tableMasterRestart, DB_NAME, 2, 3);
  }

  private void checkTable(TableMaster tableMaster, String dbName, int numTables, int numPartitions)
      throws IOException {
    assertEquals(numTables, tableMaster.getAllTables(dbName).size());
    for (int i = 0; i < numTables; i++) {
      assertEquals(numPartitions, tableMaster.getTable(dbName, TestDatabase.getTableName(i))
          .getPartitions().size());
    }
  }

  @Test
  public void journalAttachDb() throws Exception {
    mClusterResource.start();
    LocalAlluxioCluster mCluster = mClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    try {
      tableMaster.getDatabase(DB_NAME);
      fail();
    } catch (IOException e) {
      assertEquals("Database " + DB_NAME + " does not exist", e.getMessage());
    }
    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap());
    assertEquals(DB_NAME, tableMaster.getDatabase(DB_NAME).getDbName());
    List<String> oldTableNames = tableMaster.getAllTables(DB_NAME);
    Table tableOld = tableMaster.getTable(DB_NAME, oldTableNames.get(0));

    mCluster.stopMasters();

    // Update Udb, the table should stay the same, until we detach / reattach
    TestDatabase.genTable(2, 2);

    mCluster.startMasters();
    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    List<String> newTableNames = tableMaster.getAllTables(DB_NAME);
    assertEquals(oldTableNames, newTableNames);
    Table tableNew = tableMasterRestart.getTable(DB_NAME, newTableNames.get(0));
    assertEquals(tableOld.getName(), tableNew.getName());
  }

  @Test
  public void journalDetachDb() throws Exception {
    mClusterResource.start();
    LocalAlluxioCluster mCluster = mClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);

    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap());
    tableMaster.detachDatabase(DB_NAME);
    assertTrue(tableMaster.getAllDatabases().isEmpty());
    mCluster.stopMasters();
    TestDatabase.genTable(2, 2);
    mCluster.startMasters();
    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    assertTrue(tableMasterRestart.getAllDatabases().isEmpty());
  }
}
