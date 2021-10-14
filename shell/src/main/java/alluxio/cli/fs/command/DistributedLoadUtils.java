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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.job.JobConfig;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utilities Loads a file or directory in Alluxio space, makes it resident in memory.
 */
public final class DistributedLoadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLoadUtils.class);

  private DistributedLoadUtils() {} // prevent instantiation

  /**
   * Distributed loads a file or directory in Alluxio space, makes it resident in memory.
   *  @param command The command to execute loading
   * @param pool
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication Number of block replicas of each loaded file
   * @param workerSet A set of worker hosts to load data
   * @param excludedWorkerSet A set of worker hosts can not to load data
   * @param localityIds The locality identify set
   * @param excludedLocalityIds A set of worker locality identify can not to load data
   * @param printOut whether print out progress in console
   */
  public static void distributedLoad(AbstractDistributedJobCommand command,
      List<URIStatus> pool, AlluxioURI filePath, int replication, int batchSize, Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, boolean printOut)
      throws AlluxioException, IOException {
    load(command, pool,filePath, replication, batchSize, workerSet, excludedWorkerSet, localityIds,
        excludedLocalityIds, printOut);
    // Wait remaining jobs to complete.
    command.drain();
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param command The command to execute loading
   * @param pool
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication Number of block replicas of each loaded file
   * @param workerSet A set of worker hosts to load data
   * @param excludedWorkerSet A set of worker hosts can not to load data
   * @param localityIds The locality identify set
   * @param excludedLocalityIds A set of worker locality identify can not to load data
   * @param printOut whether print out progress in console
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private static void load(AbstractDistributedJobCommand command, List<URIStatus> pool,
      AlluxioURI filePath, int replication, int batchSize, Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, boolean printOut)
      throws IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();

    command.mFileSystem.iterateStatus(filePath, options, uriStatus -> {
      if (!uriStatus.isFolder()) {
        AlluxioURI fileURI = new AlluxioURI(uriStatus.getPath());
        if (uriStatus.getInAlluxioPercentage() == 100 && replication == 1) {
          // The file has already been fully loaded into Alluxio.
          if (printOut) {
            System.out.println(fileURI + " is already fully loaded in Alluxio");
          }
          return;
        }
        pool.add(uriStatus);
        if (pool.size() == batchSize) {
          addJob(command, pool, batchSize,replication, workerSet, excludedWorkerSet, localityIds,
              excludedLocalityIds, printOut);
          pool.clear();
        }
      }
    });
    // add all the jobs left in the pool
    addJob(command, pool, batchSize, replication, workerSet, excludedWorkerSet, localityIds,
        excludedLocalityIds, printOut);
  }

  private static void addJob(AbstractDistributedJobCommand command, List<URIStatus> statuses,
      int batchSize, int replication, Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, boolean printOut) {

    if (command.mSubmittedJobAttempts.size() >= command.mActiveJobs) {
      // Wait one job to complete.
      command.waitJob();
    }
    command.mSubmittedJobAttempts.add(newJob(command, statuses, batchSize,replication, workerSet,
        excludedWorkerSet, localityIds, excludedLocalityIds, printOut));
  }

  /**
   * Creates a new job to load a file in Alluxio space, makes it resident in memory.
   *  @param command The command to execute loading
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param batchSize
   * @param replication The replication of file to load into Alluxio memory
   * @param printOut whether print out progress in console
   */
  private static JobAttempt newJob(AbstractDistributedJobCommand command, List<URIStatus> filePath,
      int batchSize, int replication, Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, boolean printOut) {
    JobAttempt jobAttempt = LoadJobAttemptFactory.create(command, filePath, batchSize,replication, workerSet,
        excludedWorkerSet, localityIds, excludedLocalityIds, printOut);
    jobAttempt.run();
    return jobAttempt;
  }

  private static class LoadJobAttempt extends JobAttempt {
    private final LoadConfig mJobConfig;

    LoadJobAttempt(JobMasterClient client, LoadConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
      System.out.println(jobConfig.getFilePath() + " loading");
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {
      System.out.printf("Attempt %d to load %s failed because: %s%n",
          mRetryPolicy.getAttemptCount(), mJobConfig.getFilePath(), jobInfo.getErrorMessage());
    }

    @Override
    protected void logFailed() {
      System.out.printf("Failed to complete loading %s after %d retries.%n",
          mJobConfig.getFilePath(), mRetryPolicy.getAttemptCount());
    }

    @Override
    protected void logCompleted() {
      System.out.printf("Successfully loaded path %s after %d attempts%n",
          mJobConfig.getFilePath(), mRetryPolicy.getAttemptCount());
    }
  }

  private static class SilentLoadJobAttempt extends JobAttempt {
    private final LoadConfig mJobConfig;

    SilentLoadJobAttempt(JobMasterClient client, LoadConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {}

    @Override
    protected void logFailed() {}

    @Override
    protected void logCompleted() {}
  }

  private static class BatchedLoadJobAttempt extends JobAttempt {
    private final BatchedJobConfig mJobConfig;

    BatchedLoadJobAttempt(JobMasterClient client, BatchedJobConfig jobConfig,
        RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;

      System.out.println(jobConfig + " loading");
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {
      System.out.printf("Attempt %d to load %s failed because: %s%n",
          mRetryPolicy.getAttemptCount(), mJobConfig, jobInfo.getErrorMessage());
    }

    @Override
    protected void logFailed() {
      System.out.printf("Failed to complete loading %s after %d retries.%n",
          mJobConfig, mRetryPolicy.getAttemptCount());
    }

    @Override
    protected void logCompleted() {
      System.out.printf("Successfully loaded path %s after %d attempts%n",
          mJobConfig.getJobConfigs().stream(), mRetryPolicy.getAttemptCount());
    }
  }
  private static class SilentBatchedLoadJobAttempt extends JobAttempt {
    private final BatchedJobConfig mJobConfig;

    SilentBatchedLoadJobAttempt(JobMasterClient client, BatchedJobConfig jobConfig,
        RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {}

    @Override
    protected void logFailed() {}

    @Override
    protected void logCompleted() {}
  }
  public static class LoadJobAttemptFactory {
    public static JobAttempt create(AbstractDistributedJobCommand command, List<URIStatus> filePath,
        int batchSize, int replication, Set<String> workerSet, Set<String> excludedWorkerSet,
        Set<String> localityIds, Set<String> excludedLocalityIds, boolean printOut) {

      JobAttempt jobAttempt;
      if (batchSize == 1) {
        LoadConfig config = new LoadConfig(filePath.iterator().next().getPath(), replication,
            workerSet, excludedWorkerSet, localityIds, excludedLocalityIds);
        if (printOut) {
          jobAttempt = new LoadJobAttempt(command.mClient, config, new CountingRetry(3));
        } else {
          jobAttempt = new SilentLoadJobAttempt(command.mClient, config, new CountingRetry(3));
        }
      } else {
        HashSet<Map<String, String>> configs = Sets.newHashSet();
        ObjectMapper oMapper = new ObjectMapper();
        for (URIStatus status : filePath) {
          LoadConfig loadConfig =
              new LoadConfig(status.getPath(), replication, workerSet, excludedWorkerSet,
                  localityIds, excludedLocalityIds);
          Map<String, String> map = oMapper.convertValue(loadConfig, Map.class);
          configs.add(map);
        }
        BatchedJobConfig config = new BatchedJobConfig("Load", configs);
        if (printOut) {
          //TODO (jianjian) need to update the retry logic if we want to retry on the batch job
          jobAttempt = new BatchedLoadJobAttempt(command.mClient, config, new CountingRetry(0));
        } else {
          jobAttempt =
              new SilentBatchedLoadJobAttempt(command.mClient, config, new CountingRetry(0));
        }
      }

      return jobAttempt;
    }

  }
}
