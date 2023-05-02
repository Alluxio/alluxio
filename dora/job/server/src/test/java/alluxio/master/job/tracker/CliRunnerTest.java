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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.OperationType;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.wire.JobSource;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.metrics.DistributedCmdMetrics;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Tests {@link CliRunnerTest}.
 */
public final class CliRunnerTest {
  private static final long DEFAULT_FILE_SIZE = 100;

  private DistLoadCliRunner mLoadRunner;
  private MigrateCliRunner mMigrateRunner;
  private JobMaster mJobMaster;
  private FileSystem mFs;
  private MockedStatic<FileSystem.Factory> mMockStaticFactory;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mFs = mock(FileSystem.class);
    FileSystemContext fsCtx = mock(FileSystemContext.class);
    mMockStaticFactory = mockStatic(FileSystem.Factory.class);
    mMockStaticFactory.when(() ->
        FileSystem.Factory.create(any(FileSystemContext.class))).thenReturn(mFs);

    AlluxioConfiguration conf = mock(AlluxioConfiguration.class);

    when(fsCtx.getPathConf(any(AlluxioURI.class))).thenReturn(conf);
    when(conf.getEnum(any(PropertyKey.class), any()))
            .thenReturn(WriteType.THROUGH);

    mJobMaster = mock(JobMaster.class);
    mLoadRunner = new DistLoadCliRunner(fsCtx, mJobMaster);
    mMigrateRunner = new MigrateCliRunner(fsCtx, mJobMaster);
  }

  @After
  public void after() {
    mMockStaticFactory.close();
  }

  @Test
  public void testRunDistLoad() throws Exception {
    long fileLength = 10;
    int fileCount = 1;

    String srcString = "/src";
    AlluxioURI src = new AlluxioURI(srcString);

    URIStatus srcStatus = mock(URIStatus.class);
    List<URIStatus> listedUri = Lists.newArrayList();
    listedUri.add(srcStatus);
    List<String> listedPath = Lists.newArrayList();
    listedPath.add(srcString);

    int batchSize = 1;
    int replication = 1;
    Set<String> workerSet = Collections.EMPTY_SET;
    Set<String> excludedWorkerSet = Collections.EMPTY_SET;
    Set<String> localityIds = Collections.EMPTY_SET;
    Set<String> excludedLocalityIds = Collections.EMPTY_SET;
    boolean directCache = false;
    long jobControlId = 100;

    when(mFs.getStatus(src)).thenReturn(srcStatus);
    when(srcStatus.getLength()).thenReturn(fileLength);
    when(srcStatus.isFolder()).thenReturn(false);
    when(srcStatus.isCompleted()).thenReturn(true);
    when(srcStatus.getPath()).thenReturn(srcString);
    when(srcStatus.getInAlluxioPercentage()).thenReturn(0);

    //mock the callback on FileSystem.iterateStatus
    Mockito.doAnswer(ans -> {
      Consumer<URIStatus> callback = ans.getArgument(2);
      callback.accept(srcStatus);
      return null;
    }).when(mFs).iterateStatus(any(AlluxioURI.class),
            any(ListStatusPOptions.class), any(Consumer.class));

    // The actual CmdInfo after running through MigrateRunner.
    CmdInfo cmdInfo = mLoadRunner.runDistLoad(batchSize, src, replication, workerSet,
            excludedWorkerSet, localityIds,
            excludedLocalityIds, directCache, jobControlId);
    //only 1 attempt in this test
    CmdRunAttempt actualSingleAttempt = cmdInfo.getCmdRunAttempt().get(0);

    CmdRunAttempt expectedAttempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    expectedAttempt.setFileCount(fileCount);
    expectedAttempt.setFileSize(fileLength);
    LoadConfig config = new LoadConfig(srcString, replication, workerSet,
            excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
    expectedAttempt.setConfig(config);

    // The expected CmdInfo
    CmdInfo expected = new CmdInfo(jobControlId, OperationType.DIST_LOAD,
            JobSource.CLI, 0, listedPath);
    expected.addCmdRunAttempt(expectedAttempt);

    boolean attemptComparison = compareCmdRunAttempt(expectedAttempt, actualSingleAttempt);
    boolean compareCmdMetaInfo = compareCmdMetaInfo(cmdInfo, expected);
    Assert.assertTrue(attemptComparison);
    Assert.assertTrue(compareCmdMetaInfo);
  }

  @Test
  public void testDistLoadSetJobConfigAndFileMetrics() throws Exception {
    int fileCountLimit = 10;
    int fileNameLength = 5;
    List<URIStatus> filePath = createURIStatuses(fileCountLimit, fileNameLength);
    long expectedFileSize = filePath.size() * DEFAULT_FILE_SIZE;
    String expectedFilePath = filePath.stream()
            .map(URIStatus::getPath).collect(Collectors.joining(","));

    CmdRunAttempt attempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    try (MockedStatic<DistributedCmdMetrics> mockedDistributedCmdMetrics =
             mockStatic(DistributedCmdMetrics.class)) {
      mockedDistributedCmdMetrics.when(() ->
          DistributedCmdMetrics.getFileSize(anyString(), any(FileSystem.class),
              any(CountingRetry.class))).thenReturn(DEFAULT_FILE_SIZE);
      mLoadRunner.setJobConfigAndFileMetrics(filePath, 1, Collections.EMPTY_SET,
          Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, false, attempt);
      Assert.assertEquals(attempt.getFileSize(), expectedFileSize);
      Assert.assertEquals(attempt.getFilePath(), expectedFilePath);
    }
  }

  @Test
  public void testRunDistCp() throws Exception {
    long fileLength = 10;
    int fileCount = 1;

    String srcString = "/src";
    String dstString = "/dst";
    AlluxioURI src = new AlluxioURI(srcString);
    AlluxioURI dst = new AlluxioURI(dstString);

    URIStatus srcStatus = mock(URIStatus.class);
    List<URIStatus> listedUri = Lists.newArrayList();
    listedUri.add(srcStatus);
    List<String> listedPath = Lists.newArrayList();
    listedPath.add(srcString);

    boolean overwrite = false;
    int batchSize = 1;
    long jobControlId = 100;

    when(mFs.getStatus(src)).thenReturn(srcStatus);
    when(srcStatus.isFolder()).thenReturn(false);
    when(mFs.listStatus(src)).thenReturn(listedUri);
    when(srcStatus.getPath()).thenReturn(srcString);
    when(srcStatus.getLength()).thenReturn(fileLength);

    // The actual CmdInfo after running through MigrateRunner.
    CmdInfo cmdInfo = mMigrateRunner.runDistCp(src, dst, overwrite, batchSize, jobControlId);
    //only 1 attempt in this test
    CmdRunAttempt actualSingleAttempt = cmdInfo.getCmdRunAttempt().get(0);

    CmdRunAttempt expectedAttempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    expectedAttempt.setFileCount(fileCount);
    expectedAttempt.setFileSize(fileLength);
    MigrateConfig config = new MigrateConfig(srcString, dstString, WriteType.THROUGH, overwrite);
    expectedAttempt.setConfig(config);

    // The expected CmdInfo
    CmdInfo expected = new CmdInfo(jobControlId, OperationType.DIST_CP,
            JobSource.CLI, 0, listedPath);
    expected.addCmdRunAttempt(expectedAttempt);

    boolean attemptComparison = compareCmdRunAttempt(expectedAttempt, actualSingleAttempt);
    boolean compareCmdMetaInfo = compareCmdMetaInfo(cmdInfo, expected);

    Assert.assertTrue(attemptComparison);
    Assert.assertTrue(compareCmdMetaInfo);
  }

  @Test
  public void testDistCpSetJobConfigAndFileMetrics() {
    int fileCountLimit = 10;
    int fileNameLength = 5;
    List<Pair<String, String>> filePath = createFilePaths(fileCountLimit, fileNameLength);
    long expectedFileSize = filePath.size() * DEFAULT_FILE_SIZE;
    String expectedFilePath = filePath.stream()
        .map(Pair::getFirst).collect(Collectors.joining(","));

    CmdRunAttempt attempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    try (MockedStatic<DistributedCmdMetrics> mockedDistributedCmdMetrics =
             mockStatic(DistributedCmdMetrics.class)) {
      mockedDistributedCmdMetrics.when(() ->
          DistributedCmdMetrics.getFileSize(anyString(), any(FileSystem.class),
              any(CountingRetry.class))).thenReturn(DEFAULT_FILE_SIZE);
      mMigrateRunner.setJobConfigAndFileMetrics(filePath, false, WriteType.THROUGH, attempt);
      Assert.assertEquals(attempt.getFileSize(), expectedFileSize);
      Assert.assertEquals(attempt.getFilePath(), expectedFilePath);
    }
  }

  private List<Pair<String, String>> createFilePaths(int fileCountLimit, int nameLength) {
    List<Pair<String, String>> parts = new ArrayList<>();
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, fileCountLimit); i++) {
      String src = CommonUtils.randomAlphaNumString(nameLength);
      String dst = CommonUtils.randomAlphaNumString(nameLength);
      parts.add(new Pair<>(src, dst));
    }
    return parts;
  }

  private List<URIStatus> createURIStatuses(int fileCountLimit, int nameLength) throws Exception {
    List<URIStatus> parts = new ArrayList<>();
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, fileCountLimit); i++) {
      String src = CommonUtils.randomAlphaNumString(nameLength);
      URIStatus mock = mock(URIStatus.class);
      when(mock.getPath()).thenReturn(src);
      parts.add(mock);
    }
    return parts;
  }

  private boolean compareCmdRunAttempt(CmdRunAttempt attempt, CmdRunAttempt other) {
    return Objects.equal(attempt.getFileSize(), other.getFileSize())
            && Objects.equal(attempt.getFileCount(), other.getFileCount())
            && Objects.equal(attempt.getJobConfig(), other.getJobConfig());
  }

  private boolean compareCmdMetaInfo(CmdInfo cmdInfo, CmdInfo other) {
    return Objects.equal(cmdInfo.getJobControlId(), other.getJobControlId())
            && Objects.equal(cmdInfo.getJobSource(), other.getJobSource())
            && Objects.equal(cmdInfo.getOperationType(), other.getOperationType());
  }
}
