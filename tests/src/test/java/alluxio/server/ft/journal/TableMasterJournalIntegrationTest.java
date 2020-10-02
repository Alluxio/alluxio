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

import static alluxio.master.table.TestDatabase.genTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.grpc.table.Database;
import alluxio.grpc.table.PrincipalType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.master.table.DatabaseInfo;
import alluxio.master.table.Table;
import alluxio.master.table.TableMaster;
import alluxio.master.table.TestDatabase;
import alluxio.master.table.TestUdbFactory;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for table master functionality.
 */
public class TableMasterJournalIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;

  @ClassRule
  public static LocalAlluxioClusterResource sClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, WORKER_CAPACITY_BYTES)
          .setNumWorkers(1).build();

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TABLE_TRANSFORMATION_MONITOR);

  private static final String DB_NAME = TestDatabase.TEST_UDB_NAME;

  @Before
  public void reset() throws Exception {
    sClusterResource.get().formatAndRestartMasters();
  }

  @Test
  public void journalSync() throws Exception {
    LocalAlluxioCluster mCluster = sClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    genTable(1, 2, true);
    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap(),
            false);
    checkDb(tableMaster, DB_NAME, TestDatabase.sTestDbInfo);
    DatabaseInfo oldInfo = TestDatabase.sTestDbInfo;
    DatabaseInfo newInfo = new DatabaseInfo("test2://test2", "newowner",
        PrincipalType.ROLE, "newcomment", ImmutableMap.of("key", "value"));

    checkTable(tableMaster, DB_NAME, 1, 2);
    checkTable(tableMaster, DB_NAME, 1, 2);
    assertEquals(TestDatabase.getTableName(0), tableMaster.getAllTables(DB_NAME).get(0));
    assertEquals(1, tableMaster.getAllTables(DB_NAME).size());
    assertEquals(2, tableMaster.getTable(DB_NAME, TestDatabase.getTableName(0))
        .getPartitions().size());

    // Update Udb, the table should stay the same, until we sync
    genTable(2, 3, true);
    tableMaster.syncDatabase(DB_NAME);
    checkTable(tableMaster, DB_NAME, 2, 3);

    mCluster.stopMasters();
    mCluster.startMasters();

    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    TestDatabase.sTestDbInfo = newInfo;
    checkDb(tableMasterRestart, DB_NAME, oldInfo);
    tableMasterRestart.syncDatabase(DB_NAME);
    checkDb(tableMasterRestart, DB_NAME, newInfo);
    checkTable(tableMasterRestart, DB_NAME, 2, 3);
  }

  private void checkDb(TableMaster tableMaster, String dbName, DatabaseInfo dbInfo)
      throws IOException {
    Database db = tableMaster.getDatabase(dbName);
    assertEquals(db.getDbName(), dbName);
    assertEquals(db.getOwnerName(), dbInfo.getOwnerName());
    assertEquals(db.getOwnerType(), dbInfo.getOwnerType());
    assertEquals(db.getComment(), dbInfo.getComment());
    assertEquals(db.getLocation(), dbInfo.getLocation());
    assertEquals(db.getParameterMap(), dbInfo.getParameters());
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
    LocalAlluxioCluster mCluster = sClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    try {
      tableMaster.getDatabase(DB_NAME);
      fail();
    } catch (IOException e) {
      assertEquals("Database " + DB_NAME + " does not exist", e.getMessage());
    }
    genTable(1, 2, true);
    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap(),
            false);
    assertEquals(DB_NAME, tableMaster.getDatabase(DB_NAME).getDbName());
    List<String> oldTableNames = tableMaster.getAllTables(DB_NAME);
    Table tableOld = tableMaster.getTable(DB_NAME, oldTableNames.get(0));

    mCluster.stopMasters();
    mCluster.startMasters();
    // Update Udb, the table should stay the same, until we detach / reattach
    genTable(2, 2, true);
    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    List<String> newTableNames = tableMaster.getAllTables(DB_NAME);
    assertEquals(oldTableNames, newTableNames);
    Table tableNew = tableMasterRestart.getTable(DB_NAME, newTableNames.get(0));
    assertEquals(tableOld.getName(), tableNew.getName());
  }

  @Test
  public void journalDetachDb() throws Exception {
    LocalAlluxioCluster mCluster = sClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    genTable(1, 2, true);
    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap(),
            false);
    tableMaster.detachDatabase(DB_NAME);
    assertTrue(tableMaster.getAllDatabases().isEmpty());
    genTable(2, 2, true);
    mCluster.stopMasters();
    mCluster.startMasters();
    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    assertTrue(tableMasterRestart.getAllDatabases().isEmpty());
  }

  @Test
  public void journalTransformDb() throws Exception {
    LocalAlluxioCluster mCluster = sClusterResource.get();
    TableMaster tableMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    LocalAlluxioJobCluster jobCluster = new LocalAlluxioJobCluster();
    jobCluster.start();
    JobMaster jobMaster = jobCluster.getMaster().getJobMaster();
    genTable(1, 2, true);
    tableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB_NAME, DB_NAME, Collections.emptyMap(),
            false);
    List<String> tables = tableMaster.getAllTables(DB_NAME);

    assertFalse(tables.isEmpty());
    // all partitions are not transformed, so baselayout is the same as layout
    String tableName = tables.get(0);
    assertTrue(tableMaster.getTable(DB_NAME, tableName).getPartitions().stream().allMatch(
        partition -> partition.getBaseLayout() == partition.getLayout()));
    long jobid = tableMaster.transformTable(DB_NAME, tableName, null);
    assertNotEquals(0, jobid);
    JobTestUtils.waitForJobStatus(jobMaster, jobid, ImmutableSet.of(Status.COMPLETED,
        Status.CANCELED, Status.FAILED));
    final JobInfo status = jobMaster.getStatus(jobid);
    assertEquals("", status.getErrorMessage());
    assertEquals(Status.COMPLETED, status.getStatus());
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TABLE_TRANSFORMATION_MONITOR);
    // all partitions are transformed, so baselayout should be different as layout
    assertTrue(tableMaster.getTable(DB_NAME, tableName).getPartitions().stream().allMatch(
        partition -> partition.getBaseLayout() != partition.getLayout()));
    mCluster.stopMasters();
    mCluster.startMasters();
    genTable(1, 4, true);
    TableMaster tableMasterRestart =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(TableMaster.class);
    Table table = tableMaster.getTable(DB_NAME, tableName);
    // all partitions remain transformed
    assertTrue(tableMaster.getTable(DB_NAME, tableName).getPartitions().stream().allMatch(
        partition -> partition.getBaseLayout() != partition.getLayout()));
    tableMasterRestart.syncDatabase(DB_NAME);
    // The first two partitions should remain transformed, the new partitions are not transformed
    assertTrue(tableMaster.getTable(DB_NAME, tableName).getPartitions().stream().allMatch(
        partition -> (partition.getSpec().endsWith("0") || partition.getSpec().endsWith("1"))
            == (partition.getBaseLayout() != partition.getLayout())));
  }
}
