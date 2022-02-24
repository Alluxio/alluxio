package alluxio.client.cli.job;

import static org.junit.Assert.assertEquals;

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
  private static final String NOOP_PLAN = "/compact";

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
  public void before() {
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
  }

  private void runJob() throws Exception {
  //  expectException(IOException.class, ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("No such path"));
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
  }

  private void expectException(Class<? extends Throwable> cls, String msg) {
    mException.expect(cls);
    mException.expectMessage(msg);
  }

  @Test
  public void testSupportedConfigForCompleteStatus() throws Exception {
    runJob();
    //Run for supported configs.
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mLoadJobConfig, mFileSystem, mRetryPolicy);
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mMigrateJobConfig, mFileSystem, mRetryPolicy);
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mPersistJobConfig, mFileSystem, mRetryPolicy);

    ////should expect function calls to fileSystem.getStatus(new AlluxioURI(filePath)).getLength(),
    // and cooresponding file sizes obtained.
    double loadFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double migrateFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();
    double persistFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    assertEquals(loadFileSize, LOAD_FILE_LENGTH, 0);
    assertEquals(migrateFileSize, MIGRATE_FILE_LENGTH, 0);
    assertEquals(persistFileSize, PERSIST_FILE_LENGTH, 0);
  }

  @Test
  public void testUnSupportedConfigForCompleteStatus() throws Exception {
    MetricsSystem.resetAllMetrics();
    //Run for an unsupported config NoopPlanConfig.
    DistributedCmdMetrics.incrementForCompleteStatusWithRetry(
            mNoopPlanConfig, mFileSystem, mRetryPolicy);

    double loadFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName()).getValue();
    double migrateFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName()).getValue();
    double persistFileSize = MetricsSystem.getMetricValue(
            MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName()).getValue();

    //should expect no call to fileSystem.getStatus(new AlluxioURI(filePath)).getLength().
    assertEquals(loadFileSize, DEFAULT_ZERO_LENGTH, 0);
    assertEquals(migrateFileSize, DEFAULT_ZERO_LENGTH, 0);
    assertEquals(persistFileSize, DEFAULT_ZERO_LENGTH, 0);
  }
}
