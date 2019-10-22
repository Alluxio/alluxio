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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.table.AlluxioCatalog;
import alluxio.master.table.Table;
import alluxio.master.table.TableMaster;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.common.udb.UnderDatabaseRegistry;
import alluxio.table.under.hive.HiveDatabaseFactory;
import alluxio.table.under.hive.HivePartition;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for table master functionality.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.crypto.*", "javax.security.*", "sun.security.*", "javax.net.ssl.*"})
@PrepareForTest(AlluxioCatalog.class)
public class TableMasterJournalIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false)
          .build();

  private static final String DB_NAME = "testdb";
  private static final String TABLE_NAME_PREFIX = "testtable";

  private void createMockUdbAndTable(int numOfTables) throws Exception {
    UnderDatabase udb = PowerMockito.mock(UnderDatabase.class);
    when(udb.getName()).thenReturn(DB_NAME);
    String[] tableNames = new String[numOfTables];
    Arrays.setAll(tableNames, i -> TABLE_NAME_PREFIX + Integer.toString(i));
    when(udb.getTableNames()).thenReturn(Arrays.asList(tableNames));

    for (String tableName : tableNames) {
      UdbTable udbTable = PowerMockito.mock(UdbTable.class);
      when(udbTable.getName()).thenReturn(tableName);
      when(udbTable.getOwner()).thenReturn("test");
      when(udbTable.getPartitionCols()).thenReturn(Collections.emptyList());

      when(udbTable.getLayout()).thenReturn(Layout.getDefaultInstance());
      when(udbTable.getParameters()).thenReturn(Collections.emptyMap());
      when(udbTable.getSchema()).thenReturn(Schema.getDefaultInstance());
      when(udbTable.getPartitions()).thenReturn(Arrays.asList(new HivePartition(
          new HiveLayout(PartitionInfo.newBuilder().setDbName(DB_NAME)
              .setTableName(tableName).setStorage(Storage.getDefaultInstance()).build(),
              Collections.emptyList()))));
      when(udb.getTable(tableName)).thenReturn(udbTable);
    }

    UnderDatabaseRegistry registry = PowerMockito.mock(UnderDatabaseRegistry.class);

    doNothing().when(registry).refresh();
    when(registry.create(any(UdbContext.class), anyString(), any(UdbConfiguration.class)))
        .thenReturn(udb);
    PowerMockito.whenNew(UnderDatabaseRegistry.class).withNoArguments().thenReturn(registry);
  }

  @Before
  public void before() throws Exception {
    createMockUdbAndTable(1);
  }

  @Test
  public void journalAttachandDetachDb() throws Exception {
    mClusterResource.start();
    LocalAlluxioCluster mCluster = mClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);

    tableMaster.attachDatabase(DB_NAME, HiveDatabaseFactory.TYPE, Collections.emptyMap());
    List<String> oldTableNames = tableMaster.getAllTables(DB_NAME);
    Table tableOld = tableMaster.getTable(DB_NAME, oldTableNames.get(0));

    mCluster.stopMasters();

    // Update Udb, the table should stay the same, until we detach / reattach
    createMockUdbAndTable(2);

    mCluster.startMasters();
    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    List<String> newTableNames = tableMaster.getAllTables(DB_NAME);
    assertEquals(oldTableNames, newTableNames);
    Table tableNew = tableMasterRestart.getTable(DB_NAME, newTableNames.get(0));
    assertEquals(tableOld.getName(), tableNew.getName());
    tableMasterRestart.detachDatabase(DB_NAME);
    assertTrue(tableMasterRestart.getAllDatabases().isEmpty());
    tableMasterRestart.attachDatabase(DB_NAME, HiveDatabaseFactory.TYPE, Collections.emptyMap());

    List<String> reattachedTableNames = tableMasterRestart.getAllTables(DB_NAME);
    assertNotEquals(oldTableNames, reattachedTableNames);
  }
}
