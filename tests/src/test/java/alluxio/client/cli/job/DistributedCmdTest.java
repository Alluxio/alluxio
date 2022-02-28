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
import static org.mockito.Mockito.never;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.job.plan.NoopPlanConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.master.job.metrics.DistributedCmdMetrics;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class DistributedCmdTest {
  private static final String LOAD = "/load";
  private static final String MIGRATE = "/migrate";
  private static final String PERSIST = "/persist";
  private static final String NOOP_PLAN = "/noop";

  private static final long LOAD_FILE_LENGTH = 1;
  private static final long MIGRATE_FILE_LENGTH = 2;
  private static final long PERSIST_FILE_LENGTH = 3;
  private static final long NOOP_PLAN_FILE_LENGTH = 4;
  private static final long DEFAULT_ZERO_LENGTH = 0;

  private LoadConfig mLoadJobConfig;
  private MigrateConfig mMigrateJobConfig;
  private PersistConfig mPersistJobConfig;
  private NoopPlanConfig mNoopPlanConfig;
  private FileSystem mFileSystem;
  private RetryPolicy mRetryPolicy;

  private URIStatus mLoadURIStatus;
  private URIStatus mMigrateURIStatus;
  private URIStatus mPersistURIStatus;
  private URIStatus mNoopPlanURIStatus;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mLoadJobConfig = Mockito.mock(LoadConfig.class);
    mMigrateJobConfig = Mockito.mock(MigrateConfig.class);
    mPersistJobConfig = Mockito.mock(PersistConfig.class);
    mNoopPlanConfig = Mockito.mock(NoopPlanConfig.class);
    mFileSystem = Mockito.mock(FileSystem.class);
    mRetryPolicy = new CountingRetry(5);

    mLoadURIStatus = Mockito.mock(URIStatus.class);
    mMigrateURIStatus = Mockito.mock(URIStatus.class);
    mPersistURIStatus = Mockito.mock(URIStatus.class);
    mNoopPlanURIStatus = Mockito.mock(URIStatus.class);

    mockFsOnConfigs();
    MetricsSystem.resetAllMetrics();
  }

  private void mockFsOnConfigs() throws Exception {
    Mockito.when(mLoadURIStatus.getLength()).thenReturn(LOAD_FILE_LENGTH);

    Mockito.when(mLoadJobConfig.getFilePath()).thenReturn(LOAD);
    Mockito.when(mMigrateJobConfig.getSource()).thenReturn(MIGRATE);
    Mockito.when(mPersistJobConfig.getFilePath()).thenReturn(PERSIST);

    Mockito.when(mFileSystem.getStatus(new AlluxioURI(LOAD))).thenReturn(mLoadURIStatus);
    Mockito.when(mFileSystem.getStatus(new AlluxioURI(LOAD)).getLength())
            .thenReturn(LOAD_FILE_LENGTH);

    Mockito.when(mFileSystem.getStatus(new AlluxioURI(MIGRATE))).thenReturn(mMigrateURIStatus);
    Mockito.when(mFileSystem.getStatus(new AlluxioURI(MIGRATE)).getLength())
            .thenReturn(MIGRATE_FILE_LENGTH);

    Mockito.when(mFileSystem.getStatus(new AlluxioURI(PERSIST))).thenReturn(mPersistURIStatus);
    Mockito.when(mFileSystem.getStatus(new AlluxioURI(PERSIST)).getLength())
            .thenReturn(PERSIST_FILE_LENGTH);

    Mockito.when(mFileSystem.getStatus(new AlluxioURI(NOOP_PLAN))).thenReturn(mNoopPlanURIStatus);
    Mockito.when(mFileSystem.getStatus(new AlluxioURI(NOOP_PLAN)).getLength())
            .thenReturn(NOOP_PLAN_FILE_LENGTH);

    Mockito.clearInvocations(mFileSystem);
  }

  @Test
  public void testLoadConfigForCompleteStatus() throws Exception {
    // Run for load config.
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mLoadJobConfig, mFileSystem, mRetryPolicy);

    // should expect function calls to fileSystem.getStatus(new AlluxioURI(filePath)).getLength()
    // for LoadConfig only and cooresponding file sizes obtained.
    double loadFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double migrateFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();
    double persistFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    assertEquals(loadFileSize, LOAD_FILE_LENGTH, 0); // should equal LOAD_FILE_LENGTH
    assertEquals(migrateFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH
    assertEquals(persistFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH

    //should expect one call to .getStatus
    Mockito.verify(mFileSystem).getStatus(new AlluxioURI(LOAD));
  }

  @Test
  public void testMigrateConfigForCompleteStatus() throws Exception {
    // Run for migrate config.
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mMigrateJobConfig, mFileSystem, mRetryPolicy);

    // should expect function calls to fileSystem.getStatus(new AlluxioURI(filePath)).getLength()
    // for MigrateConfig only and cooresponding file sizes obtained.
    double loadFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double migrateFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();
    double persistFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    assertEquals(loadFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH
    assertEquals(migrateFileSize, MIGRATE_FILE_LENGTH, 0); // should equal MIGRATE_FILE_LENGTH
    assertEquals(persistFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH

    //should expect one call to .getStatus
    Mockito.verify(mFileSystem).getStatus(new AlluxioURI(MIGRATE));
  }

  @Test
  public void testPersistConfigForCompleteStatus() throws Exception {
    // Run for persist config.
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mPersistJobConfig, mFileSystem, mRetryPolicy);

    // should expect function calls to fileSystem.getStatus(new AlluxioURI(filePath)).getLength()
    // for PersistConfig only and cooresponding file sizes obtained.
    double loadFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double migrateFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();
    double persistFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    assertEquals(loadFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH
    assertEquals(migrateFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH
    assertEquals(persistFileSize, PERSIST_FILE_LENGTH, 0); // should equal PERSIST_FILE_LENGTH

    //should expect one call to .getStatus
    Mockito.verify(mFileSystem).getStatus(new AlluxioURI(PERSIST));
  }

  @Test
  public void testUnSupportedConfigForCompleteStatus() throws Exception {
    // Run for an unsupported config NoopPlanConfig.
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mNoopPlanConfig, mFileSystem, mRetryPolicy);

    double loadFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double migrateFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();
    double persistFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    // Should expect no call to fileSystem.getStatus(new AlluxioURI(filePath)).getLength().
    assertEquals(loadFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH
    assertEquals(migrateFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH
    assertEquals(persistFileSize, DEFAULT_ZERO_LENGTH, 0); // should equal DEFAULT_ZERO_LENGTH

    // should expect no call to .getStatus
    Mockito.verify(mFileSystem, never()).getStatus(new AlluxioURI(NOOP_PLAN));
  }
}
