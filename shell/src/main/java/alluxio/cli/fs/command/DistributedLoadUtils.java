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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Utilities Loads a file or directory in Alluxio space, makes it resident in memory.
 */
public final class DistributedLoadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLoadUtils.class);

  private DistributedLoadUtils() {} // prevent instantiation

  /**
   * Distributed loads a file or directory in Alluxio space, makes it resident in memory.
   * @param command The command to execute loading
   * @param pool The pool for batched jobs
   * @param batchSize size for batched jobs
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication Number of block replicas of each loaded file
   * @param workerSet A set of worker hosts to load data
   * @param excludedWorkerSet A set of worker hosts can not to load data
   * @param localityIds The locality identify set
   * @param excludedLocalityIds A set of worker locality identify can not to load data
   * @param directCache use direct cache request or cache through read
   * @param printOut whether print out progress in console
   */
  public static void distributedLoad(AbstractDistributedJobCommand command, List<URIStatus> pool,
      int batchSize, AlluxioURI filePath, int replication, Set<String> workerSet,
      Set<String> excludedWorkerSet, Set<String> localityIds, Set<String> excludedLocalityIds,
      boolean directCache, boolean printOut) throws AlluxioException, IOException {
    load(command, pool, batchSize, filePath, replication, workerSet, excludedWorkerSet, localityIds,
        excludedLocalityIds, directCache, printOut);
    // add all the jobs left in the pool
    if (pool.size() > 0) {
      addJob(command, pool, replication, workerSet, excludedWorkerSet, localityIds,
          excludedLocalityIds, directCache, printOut);
      pool.clear();
    }
    // Wait remaining jobs to complete.
    command.drain();
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param command The command to execute loading
   * @param pool The pool for batched jobs
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
      int batchSize, AlluxioURI filePath, int replication, Set<String> workerSet,
      Set<String> excludedWorkerSet, Set<String> localityIds, Set<String> excludedLocalityIds,
      boolean directCache, boolean printOut) throws IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    LongAdder incompleteCount = new LongAdder();
    command.mFileSystem.iterateStatus(filePath, options, uriStatus -> {
      if (!uriStatus.isFolder()) {
        if (!uriStatus.isCompleted()) {
          incompleteCount.increment();
          System.out.printf("Ignored load because: %s is in incomplete status",
              uriStatus.getPath());
          return;
        }
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
          addJob(command, pool, replication, workerSet, excludedWorkerSet, localityIds,
              excludedLocalityIds, directCache, printOut);
          pool.clear();
        }
      }
    });
    if (incompleteCount.longValue() > 0) {
      System.out.printf("Ignore load %d paths because they are in incomplete status",
          incompleteCount.longValue());
    }
  }

  private static void addJob(AbstractDistributedJobCommand command, List<URIStatus> statuses,
      int replication, Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, boolean directCache,
      boolean printOut) {
    if (command.mSubmittedJobAttempts.size() >= command.mActiveJobs) {
      // Wait one job to complete.
      command.waitJob();
    }
    command.mSubmittedJobAttempts.add(newJob(command, statuses, replication, workerSet,
        excludedWorkerSet, localityIds, excludedLocalityIds, directCache, printOut));
  }

  /**
   * Creates a new job to load a file in Alluxio space, makes it resident in memory.
   * @param command The command to execute loading
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   * @param directCache
   * @param printOut whether print out progress in console
   */
  private static JobAttempt newJob(AbstractDistributedJobCommand command, List<URIStatus> filePath,
      int replication, Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, boolean directCache,
      boolean printOut) {
    JobAttempt jobAttempt = LoadJobAttemptFactory.create(command, filePath, replication, workerSet,
        excludedWorkerSet, localityIds, excludedLocalityIds, directCache, printOut);
    jobAttempt.run();
    return jobAttempt;
  }

  private static class LoadJobAttempt extends JobAttempt {
    private final LoadConfig mJobConfig;

    LoadJobAttempt(JobMasterClient client, LoadConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
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
      System.out.printf("Successfully loaded path %s after %d attempts%n", mJobConfig.getFilePath(),
          mRetryPolicy.getAttemptCount());
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
    private final String mFilesPathString;

    BatchedLoadJobAttempt(JobMasterClient client, BatchedJobConfig jobConfig,
        RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
      String pathString = jobConfig.getJobConfigs().stream().map(x -> x.get("filePath"))
          .collect(Collectors.joining(","));
      mFilesPathString = String.format("[%s]", StringUtils.abbreviate(pathString, 80));
      System.out.printf("files: %s" + " loading", mFilesPathString);
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {
      System.out.printf("Attempt %d to load %s failed because: %s%n",
          mRetryPolicy.getAttemptCount(), mFilesPathString, jobInfo.getErrorMessage());
    }

    @Override
    protected void logFailed() {
      System.out.printf("Failed to complete loading %s after %d retries.%n", mFilesPathString,
          mRetryPolicy.getAttemptCount());
    }

    @Override
    protected void logCompleted() {
      System.out.printf("Successfully loaded path %s after %d attempts%n", mFilesPathString,
          mRetryPolicy.getAttemptCount());
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

  /**
   * Factory class for creating different load job attempt.
   */
  public static class LoadJobAttemptFactory {
    /**
     * Loads a file or directory in Alluxio space, makes it resident in memory.
     * @param command The command to execute loading
     * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
     * @param replication Number of block replicas of each loaded file
     * @param workerSet A set of worker hosts to load data
     * @param excludedWorkerSet A set of worker hosts can not to load data
     * @param localityIds The locality identify set
     * @param excludedLocalityIds A set of worker locality identify can not to load data
     * @param directCache use direct cache request or cache through read
     * @param printOut whether print out progress in console
     * @return specific load job attempt
     **/
    public static JobAttempt create(AbstractDistributedJobCommand command, List<URIStatus> filePath,
        int replication, Set<String> workerSet, Set<String> excludedWorkerSet,
        Set<String> localityIds, Set<String> excludedLocalityIds, boolean directCache,
        boolean printOut) {
      int poolSize = filePath.size();
      JobAttempt jobAttempt;
      if (poolSize == 1) {
        LoadConfig config = new LoadConfig(filePath.iterator().next().getPath(), replication,
            workerSet, excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
        if (printOut) {
          System.out.println(config.getFilePath() + " loading");
          jobAttempt = new LoadJobAttempt(command.mClient, config, new CountingRetry(3));
        } else {
          jobAttempt = new SilentLoadJobAttempt(command.mClient, config, new CountingRetry(3));
        }
      } else {
        HashSet<Map<String, String>> configs = Sets.newHashSet();
        ObjectMapper oMapper = new ObjectMapper();
        for (URIStatus status : filePath) {
          LoadConfig loadConfig = new LoadConfig(status.getPath(), replication, workerSet,
              excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
          Map<String, String> map = oMapper.convertValue(loadConfig, Map.class);
          configs.add(map);
        }
        BatchedJobConfig config = new BatchedJobConfig("Load", configs);
        if (printOut) {
          jobAttempt = new BatchedLoadJobAttempt(command.mClient, config, new CountingRetry(3));
        } else {
          jobAttempt =
              new SilentBatchedLoadJobAttempt(command.mClient, config, new CountingRetry(3));
        }
      }
      return jobAttempt;
    }
  }
}

