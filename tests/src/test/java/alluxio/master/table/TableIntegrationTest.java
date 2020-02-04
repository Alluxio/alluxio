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

import alluxio.AlluxioTestDirectory;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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

public final class TableIntegrationTest extends BaseIntegrationTest {
  private static final String DB_NAME = "test";
  private static final File WAREHOUSE_DIR = AlluxioTestDirectory
      .createTemporaryDirectory(new File("/tmp/alluxio-tests"), "TableIntegrationTest");

  @ClassRule
  public static TemporaryFolder sFolder = new TemporaryFolder();

  @ClassRule
  public static GenericContainer sHms =
      new GenericContainer<>("genepang/hms:latest").withExposedPorts(9083)
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
    new File(dbDir.getAbsolutePath() + "/test_table").mkdirs();
    PrintWriter writer = new PrintWriter(dbDir.getAbsolutePath() + "/test_table/data.csv");
    writer.println("1,11,111,1.1,1111,11111");
    writer.println("2,22,222,2.2,2222,22222");
    writer.close();

    // write data files for table: test_table_part
    new File(dbDir.getAbsolutePath() + "/test_table_part/partitionint6=11111").mkdirs();
    writer =
        new PrintWriter(dbDir.getAbsolutePath() + "/test_table_part/partitionint6=11111/data.csv");
    writer.println("1,11,111,1.1,1111");
    writer.close();
    new File(dbDir.getAbsolutePath() + "/test_table_part/partitionint6=22222").mkdirs();
    writer =
        new PrintWriter(dbDir.getAbsolutePath() + "/test_table_part/partitionint6=22222/data.csv");
    writer.println("2,22,222,2.2,2222");
    writer.close();

    String[] lines = {
        String.format("CREATE DATABASE %s; USE %s;", DB_NAME, DB_NAME),
        String.format(
            "CREATE EXTERNAL TABLE test_table(`int1` int, `long2` bigint, `string3` string, "
                + "`float4` float, " + "`long5` bigint, `partitionint6` int) LOCATION 'file://%s';",
            dbDir.getAbsolutePath() + "/test_table"),
        String.format(
            "CREATE EXTERNAL TABLE test_table_part(`int1` int, `long2` bigint, `string3` string, "
                + "`float4` "
                + "float, `long5` bigint) PARTITIONED BY (partitionint6 INT) LOCATION 'file://%s';",
            dbDir.getAbsolutePath() + "/test_table_part"),
        "MSCK REPAIR TABLE test_table_part;",
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
    Table table = sTableMaster.getTable(DB_NAME, "test_table");
    long version = table.getVersion();
    table = sTableMaster.getTable(DB_NAME, "test_table_part");
    long versionPart = table.getVersion();

    sTableMaster.syncDatabase(DB_NAME);
    assertEquals(version, sTableMaster.getTable(DB_NAME, "test_table").getVersion());
    assertEquals(versionPart, sTableMaster.getTable(DB_NAME, "test_table_part").getVersion());
  }

  @Test
  public void syncModifiedTable() throws Exception {
    Table table = sTableMaster.getTable(DB_NAME, "test_table");
    long version = table.getVersion();
    table = sTableMaster.getTable(DB_NAME, "test_table_part");
    long versionPart = table.getVersion();

    String[] lines = {
        String.format("USE %s;", DB_NAME),
        String.format("ALTER TABLE test_table SET LOCATION 'file://%s/test.db/test_table';",
            WAREHOUSE_DIR.getAbsolutePath()),
        String.format(
            "ALTER TABLE test_table_part partition(partitionint6=22222) SET LOCATION "
                + "'file://%s/test.db/test_table_part/partitionint6=22222/';",
            WAREHOUSE_DIR.getAbsolutePath()),
    };
    execBeeline(Arrays.asList(lines));

    sTableMaster.syncDatabase(DB_NAME);
    long newVersion = sTableMaster.getTable(DB_NAME, "test_table").getVersion();
    long newVersionPart = sTableMaster.getTable(DB_NAME, "test_table_part").getVersion();
    assertNotEquals(version, newVersion);
    assertNotEquals(versionPart, newVersionPart);
    sTableMaster.syncDatabase(DB_NAME);
    assertEquals(newVersion, sTableMaster.getTable(DB_NAME, "test_table").getVersion());
    assertEquals(newVersionPart, sTableMaster.getTable(DB_NAME, "test_table_part").getVersion());
  }

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
