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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.DefaultSafeModeManager;
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.SafeModeManager;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.time.ExponentialTimer;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.FileInfo;
import alluxio.worker.job.JobMasterClientConfig;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobMasterClient.Factory.class)
public final class PersistenceTest {
  private File mJournalFolder;
  private MasterRegistry mRegistry;
  private FileSystemMaster mFileSystemMaster;
  private JobMasterClient mMockJobMasterClient;
  private SafeModeManager mSafeModeManager;
  private long mStartTimeMs;
  private int mPort;
  private static final GetStatusContext GET_STATUS_CONTEXT = GetStatusContext.defaults();

  @Rule
  public ManuallyScheduleHeartbeat mManualScheduler =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_PERSISTENCE_CHECKER,
          HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);

  @Before
  public void before() throws Exception {
    AuthenticatedClientUser.set(LoginUser.get().getName());
    TemporaryFolder tmpFolder = new TemporaryFolder();
    tmpFolder.create();
    File ufsRoot = tmpFolder.newFolder();
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, ufsRoot.getAbsolutePath());
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, 0);
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_MAX_INTERVAL_MS, 1000);
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS, 0);
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS, 1000);
    mJournalFolder = tmpFolder.newFolder();
    mSafeModeManager = new DefaultSafeModeManager();
    mStartTimeMs = System.currentTimeMillis();
    mPort = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
    startServices();
  }

  @After
  public void after() throws Exception {
    stopServices();
    ConfigurationTestUtils.resetConfiguration();
    AuthenticatedClientUser.remove();
  }

  @Test
  public void empty() throws Exception {
    checkEmpty();
  }

  @Test
  public void heartbeatEmpty() throws Exception {
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    checkEmpty();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    checkEmpty();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
    checkEmpty();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
    checkEmpty();
  }

  /**
   * Tests the progression of a successful persist job.
   */
  @Test
  public void successfulAsyncPersistence() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.CREATED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.RUNNING);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.COMPLETED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    {
      // Create the temporary UFS file.
      fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      UnderFileSystem ufs = UnderFileSystem.Factory.create(job.getTempUfsPath());
      UnderFileSystemUtils.touch(ufs, job.getTempUfsPath());
    }

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      waitUntilPersisted(testFile);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      waitUntilPersisted(testFile);
    }
  }

  /**
   * Tests that a canceled persist job is not retried.
   */
  @Test
  public void noRetryCanceled() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.CANCELED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkEmpty();
    }
  }

  /**
   * Tests that a failed persist job is retried multiple times.
   */
  @Test
  public void retryFailed() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.FAILED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Repeatedly execute the persistence checker and scheduler heartbeats, checking the internal
    // state. After the internal timeout associated with the operation expires, check the operation
    // has been cancelled.
    while (true) {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceRequested(testFile);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      if (getPersistJobs().size() != 0) {
        checkPersistenceInProgress(testFile, jobId);
      } else {
        checkEmpty();
        break;
      }
      CommonUtils.sleepMs(100);
    }
    fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  /**
   * Tests that a persist file job is retried after the file is renamed and the src directory is
   * deleted.
   */
  @Test(timeout = 20000)
  public void retryPersistJobRenameDelete() throws Exception {
    AuthenticatedClientUser.set(LoginUser.get().getName());
    // Create src file and directory, checking the internal state.
    AlluxioURI alluxioDirSrc = new AlluxioURI("/src");
    mFileSystemMaster.createDirectory(alluxioDirSrc,
        CreateDirectoryContext.defaults().setPersisted(true));
    AlluxioURI alluxioFileSrc = new AlluxioURI("/src/in_alluxio");
    long fileId = mFileSystemMaster.createFile(alluxioFileSrc,
        CreateFileContext.defaults().setPersisted(false));
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(fileId).getPersistenceState());

    // Schedule the async persistence, checking the internal state.
    mFileSystemMaster.scheduleAsyncPersistence(alluxioFileSrc);
    checkPersistenceRequested(alluxioFileSrc);

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

    // Execute the persistence scheduler heartbeat, checking the internal state.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    CommonUtils.waitFor("Scheduler heartbeat", (() -> getPersistJobs().size() > 0));
    checkPersistenceInProgress(alluxioFileSrc, jobId);

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.CREATED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Execute the persistence checker heartbeat, checking the internal state.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
    CommonUtils.waitFor("Checker heartbeat", (() -> getPersistJobs().size() > 0));
    checkPersistenceInProgress(alluxioFileSrc, jobId);

    // Mock the job service interaction.
    jobInfo.setStatus(Status.COMPLETED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Create the temporary UFS file.
    {
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      PersistJob job = persistJobs.get(fileId);
      UnderFileSystem ufs = UnderFileSystem.Factory.create(job.getTempUfsPath());
      UnderFileSystemUtils.touch(ufs, job.getTempUfsPath());
    }

    // Rename the src file before the persist is commited.
    mFileSystemMaster.createDirectory(new AlluxioURI("/dst"),
        CreateDirectoryContext.defaults().setPersisted(true));
    AlluxioURI alluxioFileDst = new AlluxioURI("/dst/in_alluxio");
    mFileSystemMaster.rename(alluxioFileSrc, alluxioFileDst, RenameContext.defaults());

    // Delete the src directory recursively.
    mFileSystemMaster.delete(alluxioDirSrc,
        DeleteContext.defaults(DeletePOptions.newBuilder().setRecursive(true)));

    // Execute the persistence checker heartbeat, checking the internal state. This should
    // re-schedule the persist task as tempUfsPath is deleted.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
    CommonUtils.waitFor("Checker heartbeat", (() -> getPersistRequests().size() > 0));
    checkPersistenceRequested(alluxioFileDst);

    // Mock job service interaction.
    jobId = random.nextLong();
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

    // Execute the persistence scheduler heartbeat, checking the internal state.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    CommonUtils.waitFor("Scheduler heartbeat", (() -> getPersistJobs().size() > 0));
    checkPersistenceInProgress(alluxioFileDst, jobId);
  }

  /**
   * Tests that persist file requests are not forgotten across restarts.
   */
  @Test
  public void replayPersistRequest() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Simulate restart.
    stopServices();
    startServices();

    checkPersistenceRequested(testFile);
  }

  /**
   * Tests that persist file jobs are not forgotten across restarts.
   */
  @Test
  public void replayPersistJob() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Simulate restart.
    stopServices();
    startServices();

    checkPersistenceInProgress(testFile, jobId);
  }

  private AlluxioURI createTestFile() throws Exception {
    AlluxioURI path = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    String owner = SecurityUtils.getOwnerFromGrpcClient();
    String group = SecurityUtils.getGroupFromGrpcClient();
    mFileSystemMaster.createFile(path,
        CreateFileContext
            .defaults(
                CreateFilePOptions.newBuilder().setMode(Mode.createFullAccess().toShort()))
            .setOwner(owner).setGroup(group));
    mFileSystemMaster.completeFile(path, CompleteFileContext.defaults());
    return path;
  }

  private void checkEmpty() {
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(0, getPersistJobs().size());
  }

  private void waitUntilPersisted(final AlluxioURI testFile) throws Exception {
    // Persistence completion is asynchronous, so waiting is necessary.
    CommonUtils.waitFor("async persistence is completed for file", () -> {
      try {
        FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
        return fileInfo.getPersistenceState().equals(PersistenceState.PERSISTED.toString());
      } catch (FileDoesNotExistException | InvalidPathException | AccessControlException
          | IOException e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(30000));

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Map<Long, PersistJob> persistJobs = getPersistJobs();
    Assert.assertEquals(0, getPersistRequests().size());
    // We update the file info before removing the persist job, so we must wait here.
    CommonUtils.waitFor("persist jobs list to be empty", () -> persistJobs.isEmpty(),
        WaitForOptions.defaults().setTimeoutMs(5 * Constants.SECOND_MS));
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private void checkPersistenceInProgress(AlluxioURI testFile, long jobId) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Map<Long, PersistJob> persistJobs = getPersistJobs();
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(1, persistJobs.size());
    Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
    PersistJob job = persistJobs.get(fileInfo.getFileId());
    Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
    Assert.assertEquals(jobId, job.getId());
    Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
    Assert.assertEquals(
        PersistenceState.TO_BE_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private void checkPersistenceRequested(AlluxioURI testFile) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_CONTEXT);
    Map<Long, ExponentialTimer> persistRequests = getPersistRequests();
    Assert.assertEquals(1, persistRequests.size());
    Assert.assertEquals(0, getPersistJobs().size());
    Assert.assertTrue(persistRequests.containsKey(fileInfo.getFileId()));
    Assert.assertEquals(
        PersistenceState.TO_BE_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private Map<Long, ExponentialTimer> getPersistRequests() {
    return Whitebox.getInternalState(mFileSystemMaster, "mPersistRequests");
  }

  private Map<Long, PersistJob> getPersistJobs() {
    return Whitebox.getInternalState(mFileSystemMaster, "mPersistJobs");
  }

  private void startServices() throws Exception {
    mRegistry = new MasterRegistry();
    JournalSystem journalSystem =
        JournalTestUtils.createJournalSystem(mJournalFolder.getAbsolutePath());
    MasterContext context = MasterTestUtils.testMasterContext(journalSystem);
    new MetricsMasterFactory().create(mRegistry, context);
    new BlockMasterFactory().create(mRegistry, context);
    mFileSystemMaster = new FileSystemMasterFactory().create(mRegistry, context);
    journalSystem.start();
    journalSystem.gainPrimacy();
    mRegistry.start(true);
    mMockJobMasterClient = Mockito.mock(JobMasterClient.class);
    PowerMockito.mockStatic(JobMasterClient.Factory.class);
    Mockito.when(JobMasterClient.Factory.create(Mockito.any(JobMasterClientConfig.class)))
        .thenReturn(mMockJobMasterClient);
  }

  private void stopServices() throws Exception {
    mRegistry.stop();
  }
}
