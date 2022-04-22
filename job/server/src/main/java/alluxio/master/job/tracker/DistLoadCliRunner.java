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

package alluxio.master.job.tracker;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.OperationType;
import alluxio.job.JobConfig;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.wire.JobSource;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.metrics.DistributedCmdMetrics;
import alluxio.retry.CountingRetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * A config runner for a DistLoadCli job.
 */
public class DistLoadCliRunner extends AbstractCmdRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DistLoadCliRunner.class);

  /**
   * constructor.
   * @param fsContext
   * @param jobMaster
   */
  public DistLoadCliRunner(FileSystemContext fsContext, JobMaster jobMaster) {
    super(fsContext, jobMaster);
  }

  /**
   * Run a DistLoad command.
   * @param batchSize size for batched jobs
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication Number of block replicas of each loaded file
   * @param workerSet A set of worker hosts to load data
   * @param excludedWorkerSet A set of worker hosts can not to load data
   * @param localityIds The locality identify set
   * @param excludedLocalityIds A set of worker locality identify can not to load data
   * @param directCache Use direct cache request or cache through read
   * @param jobControlId THe parent level job control ID
   * @return CmdInfo
   */
  public CmdInfo runDistLoad(
          int batchSize, AlluxioURI filePath,
          int replication, Set<String> workerSet,
          Set<String> excludedWorkerSet, Set<String> localityIds,
          Set<String> excludedLocalityIds, boolean directCache,
          long jobControlId) throws IOException {
    long submissionTime = System.currentTimeMillis();
    List<URIStatus> filePool = new ArrayList<>(batchSize);

    //only use the source path as the file parameter.
    List<String> path = Lists.newArrayList(filePath.getPath());

    CmdInfo cmdInfo = new CmdInfo(jobControlId, OperationType.DIST_LOAD, JobSource.CLI,
            submissionTime, path);

    try {
      load(filePath, batchSize, replication, workerSet, excludedWorkerSet, localityIds,
              excludedLocalityIds, directCache, filePool, cmdInfo);
    } catch (IOException | AlluxioException e) {
      LOG.warn("failing in distcp!");
      LOG.error(e.getMessage());
      throw new IOException(e.getMessage());
    }

    // add all the jobs left in the pool.
    if (filePool.size() > 0) {
      submitDistLoad(filePool, replication, workerSet, excludedWorkerSet, localityIds,
              excludedLocalityIds, directCache, cmdInfo);
      filePool.clear();
    }
    return cmdInfo;
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication Number of block replicas of each loaded file
   * @param workerSet A set of worker hosts to load data
   * @param excludedWorkerSet A set of worker hosts can not to load data
   * @param localityIds The locality identify set
   * @param excludedLocalityIds A set of worker locality identify can not to load data
   * @param pool The pool for batched jobs
   * @param cmdInfo  Command information
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath, int batchSize, int replication, Set<String> workerSet,
                    Set<String> excludedWorkerSet, Set<String> localityIds,
                    Set<String> excludedLocalityIds, boolean directCache,
                    List<URIStatus> pool, CmdInfo cmdInfo)
          throws IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    LongAdder incompleteCount = new LongAdder();
    mFileSystem.iterateStatus(filePath, options, uriStatus -> {
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
          System.out.println(fileURI + " is already fully loaded in Alluxio");
          return;
        }
        pool.add(uriStatus);
        if (pool.size() == batchSize) {
          submitDistLoad(pool, replication, workerSet, excludedWorkerSet, localityIds,
                  excludedLocalityIds, directCache, cmdInfo);
          pool.clear();
        }
      }
    });
    if (incompleteCount.longValue() > 0) {
      System.out.printf("Ignore load %d paths because they are in incomplete status",
              incompleteCount.longValue());
    }
  }

  // Submit a child job within a distributed command job.
  private void submitDistLoad(List<URIStatus> pool, int replication,
                              Set<String> workerSet, Set<String> excludedWorkerSet,
                              Set<String> localityIds, Set<String> excludedLocalityIds,
                              boolean directCache, CmdInfo cmdInfo) {
    if (mSubmitted.size() >= DEFAULT_ACTIVE_JOBS) {
      waitForCmdJob();
    }

    CmdRunAttempt attempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    setJobConfigAndFileMetrics(pool, replication, workerSet, excludedWorkerSet,
            localityIds, excludedLocalityIds, directCache, attempt);
    mSubmitted.add(attempt);
    cmdInfo.addCmdRunAttempt(attempt);
    attempt.run();
  }

  // Create a JobConfig and set file count and size for the DistLoad job.
  protected void setJobConfigAndFileMetrics(
          List<URIStatus> filePath, int replication,
          Set<String> workerSet, Set<String> excludedWorkerSet,
          Set<String> localityIds, Set<String> excludedLocalityIds,
          boolean directCache, CmdRunAttempt attempt) {
    JobConfig jobConfig;
    long fileCount = 0;
    long fileSize = 0;
    String filePathString = filePath.stream().map(URIStatus::getPath)
            .collect(Collectors.joining(CmdJobTracker.DELIMITER));

    if (filePath.size() == 1) {
      URIStatus status = filePath.iterator().next();
      String source = status.getPath();
      jobConfig = new LoadConfig(source, replication,
              workerSet, excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
      fileCount = DEFAULT_FILE_COUNT;
      fileSize = DistributedCmdMetrics.getFileSize(source, mFileSystem, new CountingRetry(3));
    } else {
      HashSet<Map<String, String>> configs = Sets.newHashSet();
      ObjectMapper oMapper = new ObjectMapper();
      for (URIStatus status : filePath) {
        LoadConfig loadConfig = new LoadConfig(status.getPath(), replication, workerSet,
                excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
        Map<String, String> map = oMapper.convertValue(loadConfig, Map.class);
        configs.add(map);
        fileSize += DistributedCmdMetrics.getFileSize(status.getPath(),
                mFileSystem, new CountingRetry(3));
      }
      fileCount = filePath.size();
      jobConfig = new BatchedJobConfig(LoadConfig.NAME, configs);
    }
    attempt.setFileCount(fileCount);
    attempt.setFileSize(fileSize);
    attempt.setConfig(jobConfig);
    attempt.setFilePath(filePathString);
  }
}

