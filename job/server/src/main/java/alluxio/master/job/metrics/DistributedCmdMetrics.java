package alluxio.master.job.metrics;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.job.JobConfig;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.job.wire.Status;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;


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


    // Increment by counts of operations
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

    // Increment by counts of files
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

    // Increment by a fileSize
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

    //Increment failed job count
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

    //Increment cancelled job count
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

    // Increment for non-batch config for complete status
    public static void incrementForCompleteStatusWithRetry(JobConfig config, FileSystem fileSystem, int RETRY_COUNTS) {
        String jobType = config.getName();
        String filePath = getFilePathForNonBatchConfig(config);
        incrementOperationCount(jobType, 1);
        incrementFileCount(jobType, 1);

        for (int i = 0; i < RETRY_COUNTS; i++) {
            try {
                long fileSize = fileSystem.getStatus(new AlluxioURI(filePath)).getLength();
                incrementFileSize(jobType, fileSize);
                break;
            } catch (IOException | AlluxioException e) {
                i++;
                LOG.warn("Retry getStatus for URI {} for {}-th time, {}", filePath, i, Arrays.toString(e.getStackTrace()));
            }
        }
    }

    // Get file path based on each non-batch job config
    private static String getFilePathForNonBatchConfig(JobConfig config) {
        String path = null;
        if (config.getName().equals(LoadConfig.NAME)) {
            path = ((LoadConfig) config).getFilePath();
        } else if (config.getName().equals(MigrateConfig.NAME)) {
            path = ((MigrateConfig) config).getFilePath();
        } else if (config.getName().equals(PersistConfig.NAME)) {
            path = ((PersistConfig) config).getFilePath();
        }

        return path;
    }


    public static void batchIncrementForCompleteStatusWithRetry(BatchedJobConfig config, FileSystem fileSystem, int RETRY_COUNTS) {
        String jobType = config.getJobType();
        long count = config.getJobConfigs().size();

        incrementOperationCount(jobType, count);
        incrementFileCount(jobType, count);

        // filePath is defined for MigrateConfig, LoadConfig and PersistConfig currently. The "filePath" is the key to the json map inside config to get the actual file path.
        final String pathMapKey = "filePath";

        config.getJobConfigs().forEach(jobConfig -> {
                    for (int i = 0; i < RETRY_COUNTS; i++) {
                        try {
                            long fileSize = fileSystem.getStatus(new AlluxioURI(jobConfig.get(pathMapKey))).getLength();
                            incrementFileSize(jobType, fileSize);
                            break;
                        } catch (IOException | AlluxioException e) {
                            i++;
                            LOG.warn("Retry getStatus for URI {} for {}-th time, {}", pathMapKey, i, Arrays.toString(e.getStackTrace()));
                        }
                    }
                }
        );
    }

    public static void batchIncrementForFailStatus(BatchedJobConfig config) {
        String jobType = config.getJobType();
        config.getJobConfigs().forEach(jobConfig -> {
            incrementForFailStatus(jobType);
                }
        );
    }

    public static void batchIncrementForCancelStatus(BatchedJobConfig config) {
        String jobType = config.getJobType();
        config.getJobConfigs().forEach(jobConfig -> {
                    incrementForCancelStatus(jobType);
                }
        );
    }

    //check to see if the job config is batched config or not
    public static boolean isBatchConfig(JobConfig config) {
        return config.getName().equals(BatchedJobConfig.NAME);
    }
}