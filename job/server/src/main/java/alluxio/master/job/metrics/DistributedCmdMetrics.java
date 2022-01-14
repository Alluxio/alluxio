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

/**
 * Increment by counts of operations.
 * @param jobType the job type
 * @param count the count of a stats counter
 */
  public static void incrementOperationCount(String jobType, long count) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_SUCCESS.inc(count);
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_SUCCESS.inc(count);
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_SUCCESS.inc(count);
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

/**
 * Increment by counts of files.
 * @param jobType the job type
 * @param count the count of a stats counter
 */
  public static void incrementFileCount(String jobType, long count) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_FILE_COUNT.inc(count);
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_FILE_COUNT.inc(count);
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_FILE_COUNT.inc(count);
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

/**
 * Increment by a fileSize.
 * @param jobType the job type
 * @param fileSize the file size to be added
 */
  public static void incrementFileSize(String jobType, long fileSize) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_FILE_SIZE.inc(fileSize);
        JOB_DISTRIBUTED_LOAD_RATE.mark(fileSize);
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_FILE_SIZE.inc(fileSize);
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_FILE_SIZE.inc(fileSize);
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

/**
 * Increment failed job count.
 * @param jobType job type
 */
  public static void incrementForFailStatus(String jobType) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_FAIL.inc();
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_FAIL.inc();
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_FAIL.inc();
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

/**
 * Increment cancelled job count.
 * @param jobType job type
 */
  public static void incrementForCancelStatus(String jobType) {
    switch (jobType) {
      case LoadConfig.NAME:
        JOB_DISTRIBUTED_LOAD_CANCEL.inc();
        break;
      case MigrateConfig.NAME:
        MIGRATE_JOB_CANCEL.inc();
        break;
      case PersistConfig.NAME:
        ASYNC_PERSIST_CANCEL.inc();
        break;
      default:
        LOG.warn("JobType does not belong to Load, Migrate and Persist");
        break;
    }
  }

  /**
   * Increment for non-batch config for complete status.
   * @param config the job config
   * @param fileSystem filesystem
   * @param retryPolicy the retry policy used for retry operations
   */
  public static void incrementForCompleteStatusWithRetry(
          JobConfig config, FileSystem fileSystem, RetryPolicy retryPolicy) {
    String jobType = config.getName();
    String filePath = getFilePathForNonBatchConfig(config);
    incrementOperationCount(jobType, 1);
    incrementFileCount(jobType, 1);

    while (retryPolicy.attempt()) {
      try {
        long fileSize = fileSystem.getStatus(new AlluxioURI(filePath)).getLength();
        incrementFileSize(jobType, fileSize);
        break;
      } catch (IOException | AlluxioException | RuntimeException e) {
        LOG.warn("Retry getStatus for URI {} for {}-th time, {}",
                filePath, retryPolicy.getAttemptCount(), Arrays.toString(e.getStackTrace()));
      }
    }
  }

  // Get file path based on each non-batch job config
  private static String getFilePathForNonBatchConfig(JobConfig config) {
    String path = null;
    if (config instanceof LoadConfig) {
      path = ((LoadConfig) config).getFilePath();
    } else if (config instanceof MigrateConfig) {
      path = ((MigrateConfig) config).getSource();
    } else if (config instanceof PersistConfig) {
      path = ((PersistConfig) config).getFilePath();
    }

    return path;
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
    long count = config.getJobConfigs().size();

    incrementOperationCount(jobType, count);
    incrementFileCount(jobType, count);

    // filePath is defined for MigrateConfig, LoadConfig and PersistConfig currently.
    // The "filePath" is the key to the json map inside config to get the actual file path.
    String pathMapKey = null;
    if (jobType.equals(MigrateConfig.NAME)) {
      pathMapKey = "source";
    } else if (jobType.equals(LoadConfig.NAME) || jobType.equals(PersistConfig.NAME)) {
      pathMapKey = "filePath";
    }

    String finalPathMapKey = pathMapKey;
    config.getJobConfigs().forEach(jobConfig -> {
          while (retryPolicy.attempt()) {
            try {
              long fileSize = fileSystem.getStatus(
                      new AlluxioURI(jobConfig.get(finalPathMapKey))).getLength();
              incrementFileSize(jobType, fileSize);
              break;
            } catch (IOException | AlluxioException e) {
              LOG.warn("Retry getStatus for URI {} for {}-th time, {}",
                    finalPathMapKey, retryPolicy.getAttemptCount(),
                      Arrays.toString(e.getStackTrace()));
            } catch (RuntimeException e) {
              LOG.warn("Null key is found for config map with key = {}, more info is {}",
                      finalPathMapKey, Arrays.toString(e.getStackTrace()));
              break;
            }
          }
        }
    );
  }

/**
 *  Increment for Fail status for batch job.
 * @param config job config
 */
  public static void batchIncrementForFailStatus(BatchedJobConfig config) {
    String jobType = config.getJobType();
    config.getJobConfigs().forEach(jobConfig -> {
          incrementForFailStatus(jobType);
        }
    );
  }

/**
 *  Increment for Cancel status for batch job.
 * @param config job config
 */
  public static void batchIncrementForCancelStatus(BatchedJobConfig config) {
    String jobType = config.getJobType();
    config.getJobConfigs().forEach(jobConfig -> {
          incrementForCancelStatus(jobType);
        }
    );
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
      incrementForCancelStatus(config.getName());
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
      incrementForFailStatus(config.getName());
    }
  }
}
