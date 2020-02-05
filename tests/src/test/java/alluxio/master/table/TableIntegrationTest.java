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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Ignore("Enable after docker can be used from within tests")
public final class TableIntegrationTest extends BaseIntegrationTest {
  private static final String DB_NAME = "test";
  private static final File WAREHOUSE_DIR = AlluxioTestDirectory
      .createTemporaryDirectory(new File("/tmp/alluxio-tests"), "TableIntegrationTest");
  /** The docker image name and tag. */
  private static final String HMS_IMAGE = "<REPLACE WITH DOCKER IMAGE/TAG OF HMS>";
  private static final String TEST_TABLE = "test_table";
  private static final String TEST_TABLE_RENAME = "test_table_rename";
  private static final String TEST_TABLE_PART = "test_table_part";

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
    String hmsAddress = sHms.getContainerIpAddress();
    int hmsPort = sHms.getMappedPort(9083);

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
        String.format("MSCK REPAIR TABLE %s;", TEST_TABLE_RENAME),
    };

    execBeeline(Arrays.asList(lines));

    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.start();

    sTableMaster = sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(TableMaster.class);

    sTableMaster
        .attachDatabase("hive", "thrift://" + hmsAddress + ":" + hmsPort, DB_NAME, DB_NAME,
            Collections.emptyMap());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sLocalAlluxioJobCluster.stop();
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
