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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.grpc.table.SyncStatus;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.table.transform.TransformJobInfo;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Ignore("Enable after docker can be used from within tests")
public final class TableIntegrationTest extends BaseIntegrationTest {
  private static final String DB_NAME = "test";
  private static final String DB_NAME_ERRORS = "test_ERRORS";
  private static final File WAREHOUSE_DIR = AlluxioTestDirectory
      .createTemporaryDirectory(new File("/tmp/alluxio-tests"), "TableIntegrationTest");
  /** The docker image name and tag. */
  private static final String HMS_IMAGE = "<REPLACE WITH DOCKER IMAGE/TAG OF HMS>";
  private static final String TEST_TABLE = "test_table";
  private static final String TEST_TABLE_RENAME = "test_table_rename";
  private static final String TEST_TABLE_PART = "test_table_part";
  private static final String TEST_TABLE_ERROR = "table_bad_location";

  private static String sHmsAddress;
  private static int sHmsPort;

  @ClassRule
  public static TemporaryFolder sFolder = new TemporaryFolder();

  @ClassRule
  public static GenericContainer sHms =
      new GenericContainer<>(HMS_IMAGE).withExposedPorts(9083)
          .withFileSystemBind(WAREHOUSE_DIR.getAbsolutePath(), WAREHOUSE_DIR.getAbsolutePath());

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private static LocalAlluxioJobCluster sLocalAlluxioJobCluster;
  private static TableMaster sTableMaster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    sHmsAddress = sHms.getContainerIpAddress();
    sHmsPort = sHms.getMappedPort(9083);

    File dbDir = new File(WAREHOUSE_DIR, DB_NAME + ".db");
    dbDir.mkdirs();

    // write data files for the tables, to avoid the insert statement, which is slow.
    // write data files for table: test_table
    String base = dbDir.getAbsolutePath() + "/" + TEST_TABLE + "/";
    new File(base).mkdirs();
    PrintWriter writer = new PrintWriter(base + "data.csv");
    writer.println("1,11,111,1.1,1111,11111");
    writer.println("2,22,222,2.2,2222,22222");
    writer.close();

    // write data files for table: test_table_part
    base = dbDir.getAbsolutePath() + "/" + TEST_TABLE_PART + "/partitionint6=11111/";
    new File(base).mkdirs();
    writer =
        new PrintWriter(base + "data.csv");
    writer.println("1,11,111,1.1,1111");
    writer.close();
    base = dbDir.getAbsolutePath() + "/" + TEST_TABLE_PART + "/partitionint6=22222/";
    new File(base).mkdirs();
    writer =
        new PrintWriter(base + "data.csv");
    writer.println("2,22,222,2.2,2222");
    writer.close();

    // write data files for table: test_table_rename
    base = dbDir.getAbsolutePath() + "/" + TEST_TABLE_RENAME + "/";
    new File(base).mkdirs();
    writer = new PrintWriter(base + "data.csv");
    writer.println("1,11,111,1.1,1111,11111");
    writer.println("2,22,222,2.2,2222,22222");
    writer.close();

    String[] lines = {
        String.format("CREATE DATABASE %s; USE %s;", DB_NAME, DB_NAME),
        String.format(
            "CREATE EXTERNAL TABLE %s(`int1` int, `long2` bigint, `string3` string, "
                + "`float4` float, " + "`long5` bigint, `partitionint6` int) LOCATION 'file://%s';",
            TEST_TABLE, dbDir.getAbsolutePath() + "/" + TEST_TABLE),
        String.format("MSCK REPAIR TABLE %s;", TEST_TABLE),
        String.format(
            "CREATE EXTERNAL TABLE %s(`int1` int, `long2` bigint, `string3` string, "
                + "`float4` "
                + "float, `long5` bigint) PARTITIONED BY (partitionint6 INT) LOCATION 'file://%s';",
            TEST_TABLE_PART, dbDir.getAbsolutePath() + "/" + TEST_TABLE_PART),
        String.format("MSCK REPAIR TABLE %s;", TEST_TABLE_PART),
        String.format(
            "CREATE EXTERNAL TABLE %s(`int1` int, `long2` bigint, `string3` string, "
                + "`float4` float, " + "`long5` bigint, `partitionint6` int) LOCATION 'file://%s';",
            TEST_TABLE_RENAME, dbDir.getAbsolutePath() + "/" + TEST_TABLE_RENAME),

        // this database has a table which cannot be mounted.
        String.format("CREATE DATABASE %s; USE %s;", DB_NAME_ERRORS, DB_NAME_ERRORS),
        String.format(
            "CREATE EXTERNAL TABLE %s(`int1` int, `long2` bigint, `string3` string, "
                + "`float4` float, " + "`long5` bigint, `partitionint6` int) LOCATION 'file://%s';",
            TEST_TABLE, dbDir.getAbsolutePath() + "/" + TEST_TABLE),
        String.format("MSCK REPAIR TABLE %s;", TEST_TABLE),
        String.format("CREATE EXTERNAL TABLE %s(`int1` int) LOCATION 'file:///does/not/exist';",
            TEST_TABLE_ERROR),
    };

    execBeeline(Arrays.asList(lines));

    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.start();

    sTableMaster = sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(TableMaster.class);

    sTableMaster
        .attachDatabase("hive", "thrift://" + sHmsAddress + ":" + sHmsPort, DB_NAME, DB_NAME,
            Collections.emptyMap(), false);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sLocalAlluxioJobCluster.stop();
  }

  @After
  public void after() throws Exception {
    if (Sets.newHashSet(sTableMaster.getAllDatabases()).contains(DB_NAME_ERRORS)) {
      sTableMaster.detachDatabase(DB_NAME_ERRORS);
    }
  }

  @Test
  public void attachWrongDb() throws Exception {
    try {
      sTableMaster.attachDatabase("hive", "thrift://" + sHmsAddress + ":" + sHmsPort, "wrong_db",
          "wrong_db", Collections.emptyMap(), false);
      fail("Attaching wrong db name is expected to fail.");
    } catch (Exception e) {
      // this is expected
    }
  }

  @Test
  public void attachMountFail() throws Exception {
    SyncStatus status = sTableMaster
        .attachDatabase("hive", "thrift://" + sHmsAddress + ":" + sHmsPort, DB_NAME_ERRORS,
            DB_NAME_ERRORS, Collections.emptyMap(), false);
    assertEquals(1, status.getTablesErrorsCount());
    assertTrue(status.containsTablesErrors(TEST_TABLE_ERROR));
    try {
      sTableMaster.getDatabase(DB_NAME_ERRORS);
      fail("DB with a bad table should be not be attached.");
    } catch (Exception e) {
      // this is expected
    }
    status = sTableMaster
        .attachDatabase("hive", "thrift://" + sHmsAddress + ":" + sHmsPort, DB_NAME_ERRORS,
            DB_NAME_ERRORS, Collections.emptyMap(), true);
    assertEquals(1, status.getTablesErrorsCount());
    assertTrue(status.containsTablesErrors(TEST_TABLE_ERROR));
    assertNotNull(sTableMaster.getDatabase(DB_NAME_ERRORS));
  }

  @Test
  public void attachIgnore() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(CatalogProperty.DB_IGNORE_TABLES.getName(), TEST_TABLE_ERROR + ",other,");

    // expect no errors if the bad table is ignored
    SyncStatus status = sTableMaster
        .attachDatabase("hive", "thrift://" + sHmsAddress + ":" + sHmsPort, DB_NAME_ERRORS,
            DB_NAME_ERRORS, options, false);
    assertEquals(0, status.getTablesErrorsCount());
    assertEquals(1, status.getTablesIgnoredCount());

    // expect no errors on a sync, if the bad table is ignored
    status = sTableMaster.syncDatabase(DB_NAME_ERRORS);
    assertEquals(0, status.getTablesErrorsCount());
    assertEquals(1, status.getTablesIgnoredCount());
  }

  @Test
  public void syncUnmodifiedTable() throws Exception {
    Table table = sTableMaster.getTable(DB_NAME, TEST_TABLE);
    long version = table.getVersion();
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART);
    long versionPart = table.getVersion();

    sTableMaster.syncDatabase(DB_NAME);
    assertEquals(version, sTableMaster.getTable(DB_NAME, TEST_TABLE).getVersion());
    assertEquals(versionPart, sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getVersion());
  }

  @Test
  public void syncModifiedTable() throws Exception {
    Table table = sTableMaster.getTable(DB_NAME, TEST_TABLE);
    long version = table.getVersion();
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART);
    long versionPart = table.getVersion();
    Set<Long> partitionVersions =
        table.getPartitions().stream().map(Partition::getVersion).collect(Collectors.toSet());

    // update the tables and partitions
    String[] lines = {
        String.format("USE %s;", DB_NAME),
        String.format("ALTER TABLE %s SET LOCATION 'file://%s/test.db/%s';",
            TEST_TABLE, WAREHOUSE_DIR.getAbsolutePath(), TEST_TABLE),
        String.format(
            "ALTER TABLE %s partition(partitionint6=22222) SET LOCATION "
                + "'file://%s/test.db/%s/partitionint6=22222/';",
            TEST_TABLE_PART, WAREHOUSE_DIR.getAbsolutePath(), TEST_TABLE_PART),
    };
    execBeeline(Arrays.asList(lines));

    sTableMaster.syncDatabase(DB_NAME);
    long newVersion = sTableMaster.getTable(DB_NAME, TEST_TABLE).getVersion();
    long newVersionPart = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getVersion();
    Set<Long> newPartitionVersions =
        sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getPartitions().stream()
            .map(Partition::getVersion).collect(Collectors.toSet());
    assertNotEquals(version, newVersion);
    assertNotEquals(versionPart, newVersionPart);
    assertNotEquals(partitionVersions, newPartitionVersions);
    sTableMaster.syncDatabase(DB_NAME);
    assertEquals(newVersion, sTableMaster.getTable(DB_NAME, TEST_TABLE).getVersion());
    assertEquals(newVersionPart, sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getVersion());
    assertEquals(newPartitionVersions,
        sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getPartitions().stream()
            .map(Partition::getVersion).collect(Collectors.toSet()));
  }

  @Test
  public void syncModifiedTransformedTable() throws Exception {
    Table table = sTableMaster.getTable(DB_NAME, TEST_TABLE);
    long version = table.getVersion();
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART);
    long versionPart = table.getVersion();
    Set<Long> partitionVersions =
        table.getPartitions().stream().map(Partition::getVersion).collect(Collectors.toSet());

    // transform tables
    long jobId = sTableMaster.transformTable(DB_NAME, TEST_TABLE, null);
    long jobPartId = sTableMaster.transformTable(DB_NAME, TEST_TABLE_PART, null);

    String definition1 = sTableMaster.getTransformJobInfo(jobId).getDefinition();
    String definition2 = sTableMaster.getTransformJobInfo(jobPartId).getDefinition();

    CommonUtils.waitFor("Transforms to complete", () -> {
      try {
        TransformJobInfo jobInfo1 = sTableMaster.getTransformJobInfo(jobId);
        TransformJobInfo jobInfo2 = sTableMaster.getTransformJobInfo(jobPartId);
        return jobInfo1.getJobStatus() == Status.COMPLETED
            && jobInfo2.getJobStatus() == Status.COMPLETED;
      } catch (Exception e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(30000).setInterval(1000));

    // verify transformed partitions
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE);
    assertEquals(1, table.getPartitions().size());
    assertTrue(table.getPartitions().get(0).isTransformed(definition1));
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART);
    assertEquals(2, table.getPartitions().size());
    assertTrue(table.getPartitions().get(0).isTransformed(definition2));
    assertTrue(table.getPartitions().get(1).isTransformed(definition2));

    // update the tables and partitions
    String[] lines = {
        String.format("USE %s;", DB_NAME),
        String.format("ALTER TABLE %s SET LOCATION 'file://%s/test.db/%s';",
            TEST_TABLE, WAREHOUSE_DIR.getAbsolutePath(), TEST_TABLE),
        String.format(
            "ALTER TABLE %s partition(partitionint6=22222) SET LOCATION "
                + "'file://%s/test.db/%s/partitionint6=22222/';",
            TEST_TABLE_PART, WAREHOUSE_DIR.getAbsolutePath(), TEST_TABLE_PART),
    };
    execBeeline(Arrays.asList(lines));

    // verify new versions after a sync
    sTableMaster.syncDatabase(DB_NAME);
    long newVersion = sTableMaster.getTable(DB_NAME, TEST_TABLE).getVersion();
    long newVersionPart = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getVersion();
    Set<Long> newPartitionVersions =
        sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getPartitions().stream()
            .map(Partition::getVersion).collect(Collectors.toSet());
    assertNotEquals(version, newVersion);
    assertNotEquals(versionPart, newVersionPart);
    assertNotEquals(partitionVersions, newPartitionVersions);

    // verify only the updated partitions have invalidated the transformations
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE);
    assertEquals(1, table.getPartitions().size());
    assertFalse(table.getPartitions().get(0).isTransformed(definition1));
    version = table.getPartitions().get(0).getVersion();
    table = sTableMaster.getTable(DB_NAME, TEST_TABLE_PART);
    assertEquals(2, table.getPartitions().size());
    assertTrue(table.getPartitions().get(0).isTransformed(definition2)
        ^ table.getPartitions().get(1).isTransformed(definition2));
    partitionVersions =
        table.getPartitions().stream().map(Partition::getVersion).collect(Collectors.toSet());

    // verify transformed partition versions are unchanged when syncing an unmodified udb
    sTableMaster.syncDatabase(DB_NAME);
    assertEquals(version,
        sTableMaster.getTable(DB_NAME, TEST_TABLE).getPartitions().get(0).getVersion());
    newPartitionVersions =
        sTableMaster.getTable(DB_NAME, TEST_TABLE_PART).getPartitions().stream()
            .map(Partition::getVersion).collect(Collectors.toSet());
    assertEquals(partitionVersions, newPartitionVersions);
  }

  @Test
  public void renameTable() throws Exception {
    String newName = TEST_TABLE_RENAME + "_new";

    // update the tables and partitions
    String[] lines = {
        String.format("USE %s;", DB_NAME),
        String.format("ALTER TABLE %s RENAME TO %s;", TEST_TABLE_RENAME, newName),
    };
    execBeeline(Arrays.asList(lines));

    Table table = sTableMaster.getTable(DB_NAME, TEST_TABLE_RENAME);
    assertNotNull(table);
    try {
      sTableMaster.getTable(DB_NAME, newName);
      fail("getting non-existing table should not succeed.");
    } catch (Exception e) {
      // expected
    }

    sTableMaster.syncDatabase(DB_NAME);

    try {
      sTableMaster.getTable(DB_NAME, TEST_TABLE_RENAME);
      fail("previous table name should not exist after the rename.");
    } catch (Exception e) {
      // expected
    }

    table = sTableMaster.getTable(DB_NAME, newName);
    assertNotNull(table);
  }

  /**
   * Runs a sequence of commands via beeline, within the HMS container.
   *
   * @param commands the list of commands
   */
  private static void execBeeline(List<String> commands) throws Exception {
    File file = sFolder.newFile();
    PrintWriter writer = new PrintWriter(file.getAbsolutePath());
    for (String line : commands) {
      writer.println(line);
    }
    writer.close();

    sHms.copyFileToContainer(MountableFile.forHostPath(file.getAbsolutePath()),
        "/opt/" + file.getName());
    Container.ExecResult result =
        sHms.execInContainer("/opt/hive/bin/beeline", "-u", "jdbc:hive2://", "-f",
            "/opt/" + file.getName());

    if (result.getExitCode() != 0) {
      throw new IOException("beeline commands failed. stderr: " + result.getStderr());
    }
  }
}
