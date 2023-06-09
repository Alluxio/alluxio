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
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.OperationType;
import alluxio.job.JobConfig;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.wire.JobSource;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.metrics.DistributedCmdMetrics;
import alluxio.retry.CountingRetry;
import alluxio.util.io.PathUtils;

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
import java.util.stream.Collectors;

/**
 * A config runner for a MigrateCli job.
 */
public class MigrateCliRunner extends AbstractCmdRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateCliRunner.class);

  /**
   * constructor.
   * @param fsContext fs context
   * @param jobMaster job master
   */
  public MigrateCliRunner(FileSystemContext fsContext, JobMaster jobMaster) {
    super(fsContext, jobMaster);
  }

  /**
   * Run a DistCp command.
   * @param srcPath source path
   * @param dstPath destination path
   * @param overwrite overwrite file or not
   * @param batchSize batch size to run at one time
   * @param jobControlId the parent id or jobControlId
   * @return CmdInfo
   */
  public CmdInfo runDistCp(AlluxioURI srcPath, AlluxioURI dstPath,
                           boolean overwrite, int batchSize, long jobControlId) throws IOException {
    long submissionTime = System.currentTimeMillis();
    AlluxioConfiguration conf = mFsContext.getPathConf(dstPath);
    WriteType writeType = conf.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    List<Pair<String, String>> filePool = new ArrayList<>(batchSize);

    //only use the source path as the file parameter.
    List<String> filePath = Lists.newArrayList(srcPath.getPath());

    CmdInfo cmdInfo = new CmdInfo(jobControlId, OperationType.DIST_CP, JobSource.CLI,
            submissionTime, filePath);

    try {
      if (mFileSystem.getStatus(srcPath).isFolder()) {
        createFolders(srcPath, dstPath, mFileSystem);
      }
      copy(srcPath, dstPath, overwrite, batchSize, filePool, writeType, cmdInfo);
    } catch (IOException | AlluxioException e) {
      LOG.warn("failing in distcp!");
      LOG.error(e.getMessage());
      throw new IOException(e.getMessage());
    }

    // add all the jobs left in the pool
    if (filePool.size() > 0) {
      submitDistCp(filePool, overwrite, writeType, cmdInfo);
      filePool.clear();
    }
    return cmdInfo;
  }

  private void copy(AlluxioURI srcPath, AlluxioURI dstPath, boolean overwrite, int batchSize,
      List<Pair<String, String>> pool, WriteType writeType, CmdInfo cmdInfo)
          throws IOException, AlluxioException {
    for (URIStatus srcInnerStatus : mFileSystem.listStatus(srcPath)) {
      String dstInnerPath =
              computeTargetPath(srcInnerStatus.getPath(), srcPath.getPath(), dstPath.getPath());
      if (srcInnerStatus.isFolder()) {
        copy(new AlluxioURI(srcInnerStatus.getPath()), new AlluxioURI(dstInnerPath), overwrite,
                batchSize, pool, writeType, cmdInfo);
      } else {
        pool.add(new Pair<>(srcInnerStatus.getPath(), dstInnerPath));
        if (pool.size() == batchSize) {
          submitDistCp(pool, overwrite, writeType, cmdInfo);
          pool.clear();
        }
      }
    }
  }

  // Submit a child job within a distributed command job.
  private void submitDistCp(List<Pair<String, String>> pool, boolean overwrite,
         WriteType writeType, CmdInfo cmdInfo) {
    if (mSubmitted.size() >= DEFAULT_ACTIVE_JOBS) {
      waitForCmdJob();
    }

    CmdRunAttempt attempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    setJobConfigAndFileMetrics(pool, overwrite, writeType, attempt);
    mSubmitted.add(attempt);
    cmdInfo.addCmdRunAttempt(attempt);
    attempt.run();
  }

  /**
   *  Create a JobConfig and set file count and size for the Migrate job.
   * @param filePath file paths to do distCp
   * @param overwrite overwrite destination
   * @param writeType write type
   * @param attempt each child job attempt
   */
  public void setJobConfigAndFileMetrics(List<Pair<String, String>> filePath,
        boolean overwrite, WriteType writeType, CmdRunAttempt attempt) {
    int poolSize = filePath.size();
    JobConfig jobConfig;
    long fileCount = 0;
    long fileSize = 0;
    String filePathString = filePath.stream().map(Pair::getFirst)
            .collect(Collectors.joining(CmdJobTracker.DELIMITER));

    if (poolSize == 1) {
      Pair<String, String> pair = filePath.iterator().next();
      String source = pair.getFirst();
      jobConfig = new MigrateConfig(source, pair.getSecond(), writeType, overwrite);
      fileCount = DEFAULT_FILE_COUNT;
      fileSize = DistributedCmdMetrics.getFileSize(source, mFileSystem, new CountingRetry(3));
    } else {
      HashSet<Map<String, String>> configs = Sets.newHashSet();
      ObjectMapper oMapper = new ObjectMapper();
      for (Pair<String, String> pair : filePath) {
        String source = pair.getFirst();
        MigrateConfig config =
                new MigrateConfig(source, pair.getSecond(), writeType, overwrite);
        Map<String, String> map = oMapper.convertValue(config, Map.class);
        configs.add(map);
        fileSize += DistributedCmdMetrics.getFileSize(source, mFileSystem, new CountingRetry(3));
      }
      fileCount = poolSize; // file count equals poolSize.
      jobConfig = new BatchedJobConfig(MigrateConfig.NAME, configs);
    }
    attempt.setFileCount(fileCount);
    attempt.setFileSize(fileSize);
    attempt.setConfig(jobConfig);
    attempt.setFilePath(filePathString);
  }

  private void createFolders(AlluxioURI srcPath, AlluxioURI dstPath, FileSystem fileSystem)
          throws IOException, AlluxioException {
    try {
      fileSystem.createDirectory(dstPath);
      System.out.println("Created directory at " + dstPath.getPath());
    } catch (FileAlreadyExistsException e) {
      if (!fileSystem.getStatus(dstPath).isFolder()) {
        throw e;
      }
    }

    for (URIStatus srcInnerStatus : fileSystem.listStatus(srcPath)) {
      if (srcInnerStatus.isFolder()) {
        String dstInnerPath =
                computeTargetPath(srcInnerStatus.getPath(), srcPath.getPath(), dstPath.getPath());
        createFolders(new AlluxioURI(srcInnerStatus.getPath()),
                new AlluxioURI(dstInnerPath), fileSystem);
      }
    }
  }

  private String computeTargetPath(String path, String source, String destination)
          throws InvalidPathException {
    String relativePath = PathUtils.subtractPaths(path, source);

    return PathUtils.concatPath(destination, relativePath);
  }
}
