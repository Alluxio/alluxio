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

package alluxio.master.job.metrics;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.job.JobConfig;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryPolicy;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * DistributedCmdMetrics has definitions for distributed command metrics and help methods.
 */
public class DistributedCmdMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedCmdMetrics.class);

  private static final Counter JOB_DISTRIBUTED_LOAD_SUCCESS =
          MetricsSystem.counter(MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_SUCCESS.getName());
  private static final Counter JOB_DISTRIBUTED_LOAD_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FAIL.getName());
  private static final Counter JOB_DISTRIBUTED_LOAD_CANCEL =
          MetricsSystem.counter(MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_CANCEL.getName());
  private static final Counter JOB_DISTRIBUTED_LOAD_FILE_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_COUNT.getName());
  private static final Counter JOB_DISTRIBUTED_LOAD_FILE_SIZE =
          MetricsSystem.counter(MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE.getName());
  private static final Meter JOB_DISTRIBUTED_LOAD_RATE =
          MetricsSystem.meter(MetricKey.MASTER_JOB_DISTRIBUTED_LOAD_RATE.getName());

  private static final Counter MIGRATE_JOB_SUCCESS =
          MetricsSystem.counter(MetricKey.MASTER_MIGRATE_JOB_SUCCESS.getName());
  private static final Counter MIGRATE_JOB_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_MIGRATE_JOB_FAIL.getName());
  private static final Counter MIGRATE_JOB_CANCEL =
          MetricsSystem.counter(MetricKey.MASTER_MIGRATE_JOB_CANCEL.getName());
  private static final Counter MIGRATE_JOB_FILE_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_MIGRATE_JOB_FILE_COUNT.getName());
  private static final Counter MIGRATE_JOB_FILE_SIZE =
          MetricsSystem.counter(MetricKey.MASTER_MIGRATE_JOB_FILE_SIZE.getName());

  private static final Counter ASYNC_PERSIST_SUCCESS =
          MetricsSystem.counter(MetricKey.MASTER_ASYNC_PERSIST_SUCCESS.getName());
  private static final Counter ASYNC_PERSIST_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_ASYNC_PERSIST_FAIL.getName());
  private static final Counter ASYNC_PERSIST_CANCEL =
          MetricsSystem.counter(MetricKey.MASTER_ASYNC_PERSIST_CANCEL.getName());
  private static final Counter ASYNC_PERSIST_FILE_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_ASYNC_PERSIST_FILE_COUNT.getName());
  private static final Counter ASYNC_PERSIST_FILE_SIZE =
          MetricsSystem.counter(MetricKey.MASTER_ASYNC_PERSIST_FILE_SIZE.getName());

  private static final long DEFAULT_INCREMENT_VALUE = 1;

/**
 * Increment failed job count.
 * @param jobType job type
 * @param count count to increment
 */
  public static void incrementForFailStatus(String jobType, long count) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_FAIL.inc(count);
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_FAIL.inc(count);
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_FAIL.inc(count);
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

/**
 * Increment cancelled job count.
 * @param jobType job type
 * @param count count to increment
 */
  public static void incrementForCancelStatus(String jobType, long count) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_CANCEL.inc(count);
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_CANCEL.inc(count);
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_CANCEL.inc(count);
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

  /**
   * get file size for a given filePath, return 0 if exceeding retries.
   * @param filePath
   * @param fileSystem
   * @param retryPolicy
   * @return file size
   */
  public static long getFileSize(String filePath, FileSystem fileSystem, RetryPolicy retryPolicy) {
    while (retryPolicy.attempt()) {
      try {
        long fileSize = fileSystem.getStatus(new AlluxioURI(filePath)).getLength();
        return fileSize;
      } catch (IOException | AlluxioException | RuntimeException e) {
        LOG.warn("Retry getStatus for URI {} for {}-th time, {}",
                filePath, retryPolicy.getAttemptCount(), Arrays.toString(e.getStackTrace()));
      }
    }
    return 0;
  }

  /**
   * Increment for non-batch config for complete status.
   * @param config the job config
   * @param fileSystem filesystem
   * @param retryPolicy the retry policy used for retry operations
   */
  public static void incrementForCompleteStatusWithRetry(
          JobConfig config, FileSystem fileSystem, RetryPolicy retryPolicy) {
    String filePath;
    long fileSize;
    if (config instanceof LoadConfig) {
      filePath = ((LoadConfig) config).getFilePath();
      JOB_DISTRIBUTED_LOAD_SUCCESS.inc(DEFAULT_INCREMENT_VALUE);
      JOB_DISTRIBUTED_LOAD_FILE_COUNT.inc(DEFAULT_INCREMENT_VALUE);

      fileSize = getFileSize(filePath, fileSystem, retryPolicy);
      JOB_DISTRIBUTED_LOAD_FILE_SIZE.inc(fileSize);
      JOB_DISTRIBUTED_LOAD_RATE.mark(fileSize);
    } else if (config instanceof MigrateConfig) {
      filePath = ((MigrateConfig) config).getSource();
      MIGRATE_JOB_SUCCESS.inc(DEFAULT_INCREMENT_VALUE);
      MIGRATE_JOB_FILE_COUNT.inc(DEFAULT_INCREMENT_VALUE);

      fileSize = getFileSize(filePath, fileSystem, retryPolicy);
      MIGRATE_JOB_FILE_SIZE.inc(fileSize);
    } else if (config instanceof PersistConfig) {
      filePath = ((PersistConfig) config).getFilePath();
      ASYNC_PERSIST_SUCCESS.inc(DEFAULT_INCREMENT_VALUE);
      ASYNC_PERSIST_FILE_COUNT.inc(DEFAULT_INCREMENT_VALUE);

      fileSize = getFileSize(filePath, fileSystem, retryPolicy);
      ASYNC_PERSIST_FILE_SIZE.inc(fileSize);
    } else {
      LOG.warn("JobType does not belong to Load, Migrate and Persist");
    }
  }

/**
 * Increment for batch config for complete status.
 * @param config the job config
 * @param fileSystem filesystem
 * @param retryPolicy the retry policy used for retry operations
 */
  public static void batchIncrementForCompleteStatusWithRetry(
          BatchedJobConfig config, FileSystem fileSystem, RetryPolicy retryPolicy) {
    String jobType = config.getJobType();
    long count = config.getJobConfigs().size(); //count of files

    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_SUCCESS.inc(count);
        JOB_DISTRIBUTED_LOAD_FILE_COUNT.inc(count);

        for (Map<String, String> jobConfig : config.getJobConfigs()) {
          //Look up actual filePath for LoadConfig from the map, key is given "filePath"
          String filePath = jobConfig.get("filePath");
          long fileSize = getFileSize(filePath, fileSystem, retryPolicy);
          JOB_DISTRIBUTED_LOAD_FILE_SIZE.inc(fileSize);
          JOB_DISTRIBUTED_LOAD_RATE.mark(fileSize);
        }
        return;
      case MigrateConfig.NAME:
        MIGRATE_JOB_SUCCESS.inc(count);
        MIGRATE_JOB_FILE_COUNT.inc(count);

        for (Map<String, String> jobConfig : config.getJobConfigs()) {
          //Look up actual filePath for MigrateConfig from the map, key is given "source"
          String filePath = jobConfig.get("source");
          long fileSize = getFileSize(filePath, fileSystem, retryPolicy);
          MIGRATE_JOB_FILE_SIZE.inc(fileSize);
        }
        return;
      case PersistConfig.NAME:
        ASYNC_PERSIST_SUCCESS.inc(count);
        ASYNC_PERSIST_FILE_COUNT.inc(count);

        for (Map<String, String> jobConfig : config.getJobConfigs()) {
          //Look up actual filePath for PersistConfig from the map, key is given "filePath"
          String filePath = jobConfig.get("filePath");
          long fileSize = getFileSize(filePath, fileSystem, retryPolicy);
          ASYNC_PERSIST_FILE_SIZE.inc(fileSize);
        }
        return;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        return;
    }
  }

/**
 *  Increment for Fail status for batch job.
 * @param config job config
 */
  public static void batchIncrementForFailStatus(BatchedJobConfig config) {
    String jobType = config.getJobType();
    long count = config.getJobConfigs().size();
    incrementForFailStatus(jobType, count);
  }

/**
 *  Increment for Cancel status for batch job.
 * @param config job config
 */
  public static void batchIncrementForCancelStatus(BatchedJobConfig config) {
    String jobType = config.getJobType();
    long count = config.getJobConfigs().size();
    incrementForCancelStatus(jobType, count);
  }

  /**
   * Increment for both batch and non-batch configs for COMPLETE status.
   * @param config job config
   * @param fileSystem filesystem
   * @param retryPolicy the retry policy used for retry operations
   */
  public static void incrementForAllConfigsCompleteStatus(
          JobConfig config, FileSystem fileSystem, RetryPolicy retryPolicy) {
    if (config instanceof BatchedJobConfig) {
      batchIncrementForCompleteStatusWithRetry((BatchedJobConfig) config, fileSystem, retryPolicy);
    } else {
      incrementForCompleteStatusWithRetry(config, fileSystem, retryPolicy);
    }
  }

/**
 * Increment for both batch and non-batch configs for Cancel status.
  * @param config
 */
  public static void incrementForAllConfigsCancelStatus(JobConfig config) {
    if (config instanceof BatchedJobConfig) {
      batchIncrementForCancelStatus((BatchedJobConfig) config);
    } else {
      incrementForCancelStatus(config.getName(), DEFAULT_INCREMENT_VALUE);
    }
  }

/**
 * Increment for both batch and non-batch configs for Fail status.
 * @param config
 */
  public static void incrementForAllConfigsFailStatus(JobConfig config) {
    if (config instanceof BatchedJobConfig) {
      batchIncrementForFailStatus((BatchedJobConfig) config);
    } else {
      incrementForFailStatus(config.getName(), DEFAULT_INCREMENT_VALUE);
    }
  }
}
