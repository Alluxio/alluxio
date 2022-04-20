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

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.JobDoesNotExistException;
import alluxio.grpc.OperationType;
import alluxio.job.cmd.load.LoadCliConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.job.wire.CmdStatusBlock;
import alluxio.job.wire.JobSource;
import alluxio.job.wire.SimpleJobStatusBlock;
import alluxio.job.wire.Status;
import alluxio.master.job.common.CmdInfo;

import com.beust.jcommander.internal.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Tests {@link CmdJobTrackerTest}.
 */
@RunWith(PowerMockRunner.class)
public final class CmdJobTrackerTest {
  private static final int REPEATED_ATTEMPT_COUNT = 5;
  private static final int ONE_ATTEMPT = 1;

  private CmdJobTracker mCmdJobTracker;
  private FileSystem mFs;

  private long mLoadJobId;
  private long mMigrateJobId;
  private MigrateCliRunner mMigrateCliRunner;
  private DistLoadCliRunner mDistLoadRunner;
  private PersistRunner mPersistRunner;

  private LoadCliConfig mLoad;
  private MigrateCliConfig mMigrate;
  private List<Status> mSearchingCriteria = Lists.newArrayList();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mFs = mock(FileSystem.class);
    FileSystemContext fsCtx = mock(FileSystemContext.class);

    mMigrateCliRunner = PowerMockito.mock(MigrateCliRunner.class);
    mDistLoadRunner = PowerMockito.mock(DistLoadCliRunner.class);
    mPersistRunner = PowerMockito.mock(PersistRunner.class);

    mCmdJobTracker = new CmdJobTracker(fsCtx,
            mDistLoadRunner, mMigrateCliRunner, mPersistRunner);

    mLoad = new LoadCliConfig("/path/to/load", 3, 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, true);
    mMigrate = new MigrateCliConfig("/path/from", "/path/to", WriteType.THROUGH, true, 2);
    mLoadJobId = 1;
    mMigrateJobId = 2;
    mSearchingCriteria.clear();
  }

  //All tests below are for testing running progresses, job and command statuses.
  @Test
  public void runDistLoadBatchCompleteTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistLoadTest(cmdInfo, mLoad, mLoadJobId);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.COMPLETED);
  }

  @Test
  public void runDistLoadBatchFailTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.FAILED, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);

    prepareDistLoadTest(cmdInfo,  mLoad, mLoadJobId);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.FAILED);
  }

  @Test
  public void runDistLoadBatchCancelTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);

    prepareDistLoadTest(cmdInfo,  mLoad, mLoadJobId);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.CANCELED);
  }

  @Test
  public void runDistLoadBatchRunningTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.FAILED, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.RUNNING, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.CREATED, cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistLoadTest(cmdInfo,  mLoad, mLoadJobId);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.RUNNING);
  }

  @Test
  public void runDistCpBatchCompleteTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistCpTest(cmdInfo, mMigrate, mMigrateJobId);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.COMPLETED);
  }

  @Test
  public void runDistCpBatchFailTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.FAILED, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);

    prepareDistCpTest(cmdInfo, mMigrate, mMigrateJobId);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.FAILED);
  }

  @Test
  public void runDistCpBatchCancelTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);

    prepareDistCpTest(cmdInfo, mMigrate, mMigrateJobId);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.CANCELED);
  }

  @Test
  public void runDistCpBatchRunningTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareAttemptWithStatus(Status.FAILED, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.RUNNING, cmdInfo, ONE_ATTEMPT);
    prepareAttemptWithStatus(Status.CREATED, cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistCpTest(cmdInfo, mMigrate, mMigrateJobId);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.RUNNING);
  }

  @Test
  public void testFindCmdIdsForComplete() throws Exception {
    long completedId = generateMigrateCommandForStatus(Status.COMPLETED);
    mSearchingCriteria.add(Status.COMPLETED);
    Set<Long> completedCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(completedCmdIds.size(), 1);
    Assert.assertTrue(completedCmdIds.contains(completedId));
  }

  @Test
  public void testFindCmdIdsForFailed() throws Exception {
    long failedId = generateMigrateCommandForStatus(Status.FAILED);
    mSearchingCriteria.add(Status.FAILED);
    Set<Long> failedCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(failedCmdIds.size(), 1);
    Assert.assertTrue(failedCmdIds.contains(failedId));
  }

  @Test
  public void testFindCmdIdsForRunning() throws Exception {
    long runningId = generateMigrateCommandForStatus(Status.RUNNING);
    mSearchingCriteria.add(Status.RUNNING);
    Set<Long> runningCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(runningCmdIds.size(), 1);
    Assert.assertTrue(runningCmdIds.contains(runningId));
  }

  @Test
  public void testFindCmdIdsForCancel() throws Exception {
    long cancelId = generateMigrateCommandForStatus(Status.CANCELED);
    mSearchingCriteria.add(Status.CANCELED);
    Set<Long> cancelCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(cancelCmdIds.size(), 1);
    Assert.assertTrue(cancelCmdIds.contains(cancelId));
  }

  @Test
  public void testFindCmdIdsForMultipleCmds() throws Exception {
    long cancelId = generateLoadCommandForStatus(Status.CANCELED);
    long runningIdA = generateLoadCommandForStatus(Status.RUNNING);
    long runningIdB = generateLoadCommandForStatus(Status.RUNNING);
    long failedId = generateMigrateCommandForStatus(Status.FAILED);
    long completedIdA = generateMigrateCommandForStatus(Status.COMPLETED);
    long completedIB = generateMigrateCommandForStatus(Status.COMPLETED);
    long createdId = generateMigrateCommandForStatus(Status.CREATED);

    // test cancel cmd ids.
    mSearchingCriteria.add(Status.CANCELED);
    Set<Long> cancelCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(cancelCmdIds.size(), 1);
    Assert.assertTrue(cancelCmdIds.contains(cancelId));

    // test completed cmd ids.
    mSearchingCriteria.clear();
    mSearchingCriteria.add(Status.COMPLETED);
    Set<Long> completedCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(completedCmdIds.size(), 2);
    Assert.assertTrue(completedCmdIds.contains(completedIdA));
    Assert.assertTrue(completedCmdIds.contains(completedIB));

    // test failed cmd ids.
    mSearchingCriteria.clear();
    mSearchingCriteria.add(Status.FAILED);
    Set<Long> failCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(failCmdIds.size(), 1);
    Assert.assertTrue(failCmdIds.contains(failedId));

    // test running cmd ids.
    mSearchingCriteria.clear();
    mSearchingCriteria.add(Status.RUNNING);
    Set<Long> runningCmdIds = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(runningCmdIds.size(), 3); // 2 running commands + 1 created command.
    Assert.assertTrue(runningCmdIds.contains(runningIdA));
    Assert.assertTrue(runningCmdIds.contains(runningIdB));
    Assert.assertTrue(runningCmdIds.contains(createdId));

    // test running and completed cmd ids.
    mSearchingCriteria.clear();
    mSearchingCriteria.add(Status.COMPLETED);
    mSearchingCriteria.add(Status.RUNNING);
    Set<Long> ids = mCmdJobTracker.findCmdIds(mSearchingCriteria);
    Assert.assertEquals(ids.size(), 5);
    Assert.assertTrue(ids.contains(completedIdA));
    Assert.assertTrue(ids.contains(completedIB));
    Assert.assertTrue(ids.contains(runningIdA));
    Assert.assertTrue(ids.contains(runningIdB));
    Assert.assertTrue(ids.contains(createdId));
  }

  // test `getCmdStatusBlock`
  @Test
  public void testGetCmdStatusBlock() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    CmdStatusBlock expectedStatusBlock = new CmdStatusBlock(mMigrateJobId, OperationType.DIST_CP);

    int completedCount = REPEATED_ATTEMPT_COUNT;
    int runningCount = REPEATED_ATTEMPT_COUNT;
    int offset = completedCount;
    addJobStatusBlockWithStatus(Status.COMPLETED, cmdInfo, completedCount,
            expectedStatusBlock, 0);
    addJobStatusBlockWithStatus(Status.RUNNING, cmdInfo, runningCount,
            expectedStatusBlock, offset);

    prepareDistCpTest(cmdInfo, mMigrate, mMigrateJobId);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    CmdStatusBlock actualStatusBlock = mCmdJobTracker.getCmdStatusBlock(mMigrateJobId);
    Assert.assertEquals(actualStatusBlock.getJobControlId(), expectedStatusBlock.getJobControlId());
    Assert.assertEquals(actualStatusBlock.toProto(), expectedStatusBlock.toProto());
  }

 // Below are all help functions.
  private void prepareDistLoadTest(
          CmdInfo cmdInfo, LoadCliConfig loadCliConfig, long loadId) throws Exception {
    AlluxioURI filePath = new AlluxioURI(loadCliConfig.getFilePath());
    int replication = loadCliConfig.getReplication();
    Set<String> workerSet = loadCliConfig.getWorkerSet();
    Set<String> excludedWorkerSet = loadCliConfig.getExcludedWorkerSet();
    Set<String> localityIds = loadCliConfig.getLocalityIds();
    Set<String> excludedLocalityIds = loadCliConfig.getExcludedLocalityIds();
    boolean directCache = loadCliConfig.getDirectCache();
    int batch = loadCliConfig.getBatchSize();

    Mockito.when(mDistLoadRunner.runDistLoad(batch, filePath, replication, workerSet,
            excludedWorkerSet, localityIds, excludedLocalityIds, directCache, loadId))
            .thenReturn(cmdInfo);
  }

  private void prepareDistCpTest(
          CmdInfo cmdInfo, MigrateCliConfig migrateCliConfig, long migrateId) throws Exception {
    AlluxioURI src = new AlluxioURI(migrateCliConfig.getSource());
    AlluxioURI dst = new AlluxioURI(migrateCliConfig.getDestination());
    boolean overwt = migrateCliConfig.getOverWrite();
    int batch = migrateCliConfig.getBatchSize();

    Mockito.when(mMigrateCliRunner.runDistCp(src, dst, overwt, batch, migrateId))
            .thenReturn(cmdInfo);
  }

  private void prepareAttemptWithStatus(Status status, CmdInfo cmdInfo, int number) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      long jobId = new Random().nextLong();
      when(attempt.getJobId()).thenReturn(jobId); //sequential job ids.
      when(attempt.checkJobStatus()).thenReturn(status);
      cmdInfo.addCmdRunAttempt(attempt);
    }
  }

  // generate command and run the Load command, return job control ID.
  private long generateLoadCommandForStatus(Status status) throws Exception {
    long jobControlId = new Random().nextLong();
    LoadCliConfig config = new LoadCliConfig(
            "/path/to/load", 3, 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, true);

    CmdInfo cmdInfo = new CmdInfo(jobControlId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());

    switch (status) {
      case COMPLETED:
        prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
        break;
      case FAILED:
        prepareAttemptWithStatus(Status.FAILED, cmdInfo, ONE_ATTEMPT);
        break;
      case RUNNING:
        prepareAttemptWithStatus(Status.RUNNING, cmdInfo, ONE_ATTEMPT);
        break;
      case CANCELED:
        prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);
        break;
      case CREATED:
        prepareAttemptWithStatus(Status.CREATED, cmdInfo, REPEATED_ATTEMPT_COUNT);
        break;
      default:
        throw new JobDoesNotExistException("No such job");
    }

    prepareDistLoadTest(cmdInfo, config, jobControlId);
    mCmdJobTracker.run(config, jobControlId);
    return jobControlId;
  }

  // generate command and run the Migrate command, return job control ID.
  private long generateMigrateCommandForStatus(Status status) throws Exception {
    long jobControlId = new Random().nextLong();
    MigrateCliConfig config = new MigrateCliConfig(
            "/path/from", "/path/to", WriteType.THROUGH, true, 2);

    CmdInfo cmdInfo = new CmdInfo(jobControlId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());

    switch (status) {
      case COMPLETED:
        prepareAttemptWithStatus(Status.COMPLETED, cmdInfo, REPEATED_ATTEMPT_COUNT);
        break;
      case FAILED:
        prepareAttemptWithStatus(Status.FAILED, cmdInfo, ONE_ATTEMPT);
        break;
      case RUNNING:
        prepareAttemptWithStatus(Status.RUNNING, cmdInfo, ONE_ATTEMPT);
        break;
      case CANCELED:
        prepareAttemptWithStatus(Status.CANCELED, cmdInfo, ONE_ATTEMPT);
        break;
      case CREATED:
        prepareAttemptWithStatus(Status.CREATED, cmdInfo, REPEATED_ATTEMPT_COUNT);
        break;
      default:
        throw new JobDoesNotExistException("No such job");
    }

    prepareDistCpTest(cmdInfo, config, jobControlId);
    mCmdJobTracker.run(config, jobControlId);
    return jobControlId;
  }

  private void addJobStatusBlockWithStatus(
          Status status, CmdInfo cmdInfo, int number, CmdStatusBlock cmdStatusBlock, int offset) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      String filePath = String.format("filePath-%s", i + offset);
      when(attempt.getJobId()).thenReturn((long) (i + offset)); //sequential job ids.
      when(attempt.checkJobStatus()).thenReturn(status);
      when(attempt.getFilePath()).thenReturn(filePath);
      cmdInfo.addCmdRunAttempt(attempt);
      cmdStatusBlock.addJobStatusBlock(new SimpleJobStatusBlock(
              (long) (i + offset), status, filePath, ""));
    }
  }
}
