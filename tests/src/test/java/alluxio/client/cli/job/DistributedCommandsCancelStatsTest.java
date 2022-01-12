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

package alluxio.client.cli.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WritePType;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;

public class DistributedCommandsCancelStatsTest extends JobShellTest {
  @ClassRule
  public static LocalAlluxioClusterResource sResource =
      new LocalAlluxioClusterResource.Builder()
          .setNumWorkers(4)
          .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH")
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, SIZE_BYTES / 2)
          .setProperty(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, true)
          .build();

  @BeforeClass
  public static void beforeClass() throws Exception {

    sLocalAlluxioCluster = sResource.get();
    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.start();
    sFileSystem = sLocalAlluxioCluster.getClient();
    sJobMaster = sLocalAlluxioJobCluster.getMaster().getJobMaster();
    sJobShell = new alluxio.cli.job.JobShell(ServerConfiguration.global());
    sFsShell = new FileSystemShell(ServerConfiguration.global());
  }

  @Test
  public void testDistributedLoadCancelStats() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileNew", WritePType.THROUGH, 10);

    long jobId = sJobMaster.run(new LoadConfig("/testFileNew", 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, false));

    sJobShell.run("cancel", Long.toString(jobId));

    JobTestUtils
        .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED));

    sJobShell.run("stat", "-v", Long.toString(jobId));

    String[] output = mOutput.toString().split("\n");
    assertEquals(String.format("ID: %s", jobId), output[0]);
    assertEquals(String.format("Name: Load"), output[1]);
    assertTrue(output[2].contains("Description: LoadConfig"));
    assertTrue(output[2].contains("/testFileNew"));
    assertEquals("Status: CANCELED", output[3]);
    assertEquals("Task 0", output[4]);
    assertTrue(output[5].contains("\tWorker: "));
    assertEquals("\tStatus: CANCELED", output[7]);

    double cancelledCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_CANCEL.getName()).getValue();
    double failedCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FAIL.getName()).getValue();
    double completedCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_SUCCESS.getName()).getValue();
    double fileCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_COUNT.getName()).getValue();
    double fileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double loadRate = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_RATE.getName()).getValue();

    assertEquals(cancelledCount, 1, 0);
    assertEquals(failedCount, 0, 0);
    assertEquals(completedCount, 0, 0);
    assertEquals(fileCount, 0, 0);
    assertEquals(fileSize, 0, 0);
    assertEquals(loadRate, 0, 0);
  }

  @Test
  public void testDistributedCpCancelStats() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileSource", WritePType.THROUGH, 10);

    long jobId = sJobMaster.run(new MigrateConfig(
            "/testFileSource", "/testFileDest", "THROUGH", false));

    sJobShell.run("cancel", Long.toString(jobId));

    JobTestUtils
            .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED));

    sJobShell.run("stat", "-v", Long.toString(jobId));

    String[] output = mOutput.toString().split("\n");
    assertEquals(String.format("ID: %s", jobId), output[0]);
    assertEquals(String.format("Name: Migrate"), output[1]);
    assertTrue(output[2].contains("Description: MigrateConfig"));
    assertTrue(output[2].contains("/testFileSource"));
    assertTrue(output[2].contains("/testFileDest"));
    assertEquals("Status: CANCELED", output[3]);
    assertEquals("Task 0", output[4]);
    assertTrue(output[5].contains("\tWorker: "));
    assertEquals("\tStatus: CANCELED", output[7]);

    double cancelledCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_CANCEL.getName()).getValue();
    double failedCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FAIL.getName()).getValue();
    double completedCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_SUCCESS.getName()).getValue();
    double fileCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_COUNT.getName()).getValue();
    double fileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();

    assertEquals(cancelledCount, 1, 0);
    assertEquals(failedCount, 0, 0);
    assertEquals(completedCount, 0, 0);
    assertEquals(fileCount, 0, 0);
    assertEquals(fileSize, 0, 0);
  }

  @Test
  public void testAsyncPersistCancelStats() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.THROUGH, 10);

    long jobId = sJobMaster.run(new PersistConfig("/testFile", 0, false, "/testUfsPath"));

    sJobShell.run("cancel", Long.toString(jobId));

    JobTestUtils
            .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED));

    sJobShell.run("stat", "-v", Long.toString(jobId));

    String[] output = mOutput.toString().split("\n");
    assertEquals(String.format("ID: %s", jobId), output[0]);
    assertEquals(String.format("Name: Persist"), output[1]);
    assertTrue(output[2].contains("Description: PersistConfig"));
    assertTrue(output[2].contains("/testFile"));
    assertEquals("Status: CANCELED", output[3]);
    assertEquals("Task 0", output[4]);
    assertTrue(output[5].contains("\tWorker: "));
    assertEquals("\tStatus: CANCELED", output[6]);

    double cancelledCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_CANCEL.getName()).getValue();
    double failedCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FAIL.getName()).getValue();
    double completedCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_SUCCESS.getName()).getValue();
    double fileCount = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_COUNT.getName()).getValue();
    double fileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    assertEquals(cancelledCount, 1, 0);
    assertEquals(failedCount, 0, 0);
    assertEquals(completedCount, 0, 0);
    assertEquals(fileCount, 0, 0);
    assertEquals(fileSize, 0, 0);
  }
}
