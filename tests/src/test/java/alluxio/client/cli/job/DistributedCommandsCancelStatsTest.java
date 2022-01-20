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

import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WritePType;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;

import com.google.common.collect.Sets;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests stat counter values and output of CANCEL operations for distributed commands.
 * The tests will depend on the timing of the actual job runs.
 * If the job completes fast enough before the CANCEL operations runs,then the test would fail.
 * The tests compare the job statuses (CANCEL or not) and stat counter values for each status.
 */
public class DistributedCommandsCancelStatsTest extends JobShellTest {
  private static final long SLEEP_MS = Constants.SECOND_MS / 2;
  private static final int TEST_TIMEOUT = 45;

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
        new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
            new SleepingUnderFileSystemOptions()
                .setGetStatusMs(SLEEP_MS)
                .setExistsMs(SLEEP_MS)
                .setListStatusMs(SLEEP_MS)
                .setListStatusWithOptionsMs(SLEEP_MS)));

  @Test
  public void testDistributedLoadCancelStats() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileNew", WritePType.THROUGH, 10);

    long jobId = sJobMaster.run(new LoadConfig("/testFileNew", 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, false));

    sJobShell.run("cancel", Long.toString(jobId));

    JobTestUtils
        .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);

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
            .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);

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
            .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);

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
