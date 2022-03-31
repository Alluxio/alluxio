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
import alluxio.grpc.OperationType;
import alluxio.job.cmd.load.LoadCliConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.job.wire.JobSource;
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
  }

  @Test
  public void runDistLoadBatchCompleteTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistLoadTest(cmdInfo);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.COMPLETED);
  }

  @Test
  public void runDistLoadBatchFailTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareFailedAttempt(cmdInfo, ONE_ATTEMPT);
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareCanceledAttempt(cmdInfo, ONE_ATTEMPT);

    prepareDistLoadTest(cmdInfo);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.FAILED);
  }

  @Test
  public void runDistLoadBatchCancelTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareCanceledAttempt(cmdInfo, ONE_ATTEMPT);

    prepareDistLoadTest(cmdInfo);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.CANCELED);
  }

  @Test
  public void runDistLoadBatchRunningTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mLoadJobId, OperationType.DIST_LOAD,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareFailedAttempt(cmdInfo, ONE_ATTEMPT);
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareCanceledAttempt(cmdInfo, ONE_ATTEMPT);
    prepareRunningAttempt(cmdInfo, ONE_ATTEMPT);
    prepareCreatedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistLoadTest(cmdInfo);

    mCmdJobTracker.run(mLoad, mLoadJobId);
    Status s = mCmdJobTracker.getCmdStatus(mLoadJobId);
    Assert.assertEquals(s, Status.RUNNING);
  }

  @Test
  public void runDistCpBatchCompleteTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistCpTest(cmdInfo);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.COMPLETED);
  }

  @Test
  public void runDistCpBatchFailTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareFailedAttempt(cmdInfo, ONE_ATTEMPT);
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareCanceledAttempt(cmdInfo, ONE_ATTEMPT);

    prepareDistCpTest(cmdInfo);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.FAILED);
  }

  @Test
  public void runDistCpBatchCancelTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareCanceledAttempt(cmdInfo, ONE_ATTEMPT);

    prepareDistCpTest(cmdInfo);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.CANCELED);
  }

  @Test
  public void runDistCpBatchRunningTest() throws Exception {
    CmdInfo cmdInfo = new CmdInfo(mMigrateJobId, OperationType.DIST_CP,
            JobSource.CLI, System.currentTimeMillis(), Lists.newArrayList());
    prepareFailedAttempt(cmdInfo, ONE_ATTEMPT);
    prepareCompletedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);
    prepareCanceledAttempt(cmdInfo, ONE_ATTEMPT);
    prepareRunningAttempt(cmdInfo, ONE_ATTEMPT);
    prepareCreatedAttempt(cmdInfo, REPEATED_ATTEMPT_COUNT);

    prepareDistCpTest(cmdInfo);

    mCmdJobTracker.run(mMigrate, mMigrateJobId);
    Status s = mCmdJobTracker.getCmdStatus(mMigrateJobId);
    Assert.assertEquals(s, Status.RUNNING);
  }

  private void prepareDistLoadTest(CmdInfo cmdInfo) throws Exception {
    AlluxioURI filePath = new AlluxioURI(mLoad.getFilePath());
    int replication = mLoad.getReplication();
    Set<String> workerSet = mLoad.getWorkerSet();
    Set<String> excludedWorkerSet = mLoad.getExcludedWorkerSet();
    Set<String> localityIds = mLoad.getLocalityIds();
    Set<String> excludedLocalityIds = mLoad.getExcludedLocalityIds();
    boolean directCache = mLoad.getDirectCache();
    int batch = mLoad.getBatchSize();

    Mockito.when(mDistLoadRunner.runDistLoad(batch, filePath, replication, workerSet,
            excludedWorkerSet, localityIds, excludedLocalityIds, directCache, mLoadJobId))
            .thenReturn(cmdInfo);
  }

  private void prepareDistCpTest(CmdInfo cmdInfo) throws Exception {
    AlluxioURI src = new AlluxioURI(mMigrate.getSource());
    AlluxioURI dst = new AlluxioURI(mMigrate.getDestination());
    boolean overwt = mMigrate.getOverWrite();
    int batch = mMigrate.getBatchSize();

    Mockito.when(mMigrateCliRunner.runDistCp(src, dst, overwt, batch, mMigrateJobId))
            .thenReturn(cmdInfo);
  }

  private void prepareCompletedAttempt(CmdInfo cmdInfo, int number) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      when(attempt.checkJobStatus()).thenReturn(Status.COMPLETED);
      cmdInfo.addCmdRunAttempt(attempt);
    }
  }

  private void prepareFailedAttempt(CmdInfo cmdInfo, int number) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      when(attempt.checkJobStatus()).thenReturn(Status.FAILED);
      cmdInfo.addCmdRunAttempt(attempt);
    }
  }

  private void prepareCanceledAttempt(CmdInfo cmdInfo, int number) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      when(attempt.checkJobStatus()).thenReturn(Status.CANCELED);
      cmdInfo.addCmdRunAttempt(attempt);
    }
  }

  private void prepareRunningAttempt(CmdInfo cmdInfo, int number) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      when(attempt.checkJobStatus()).thenReturn(Status.RUNNING);
      cmdInfo.addCmdRunAttempt(attempt);
    }
  }

  private void prepareCreatedAttempt(CmdInfo cmdInfo, int number) {
    for (int i = 0; i < number; i++) {
      CmdRunAttempt attempt = mock(CmdRunAttempt.class);
      when(attempt.checkJobStatus()).thenReturn(Status.CREATED);
      cmdInfo.addCmdRunAttempt(attempt);
    }
  }
}
