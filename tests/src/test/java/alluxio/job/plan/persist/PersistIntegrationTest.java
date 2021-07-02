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

package alluxio.job.plan.persist;

import static alluxio.job.wire.Status.COMPLETED;
import static alluxio.job.wire.Status.FAILED;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.JobIntegrationTest;
import alluxio.job.wire.JobInfo;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.job.JobMaster;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link PersistDefinition}.
 */
public final class PersistIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";
  private static final Mode TEST_MODE = new Mode((short) 0777);

  /**
   * Tests persisting a file.
   */
  @Test
  public void persistTest() throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    // run the persist job and check that it succeeds
    waitForJobToFinish(mJobMaster.run(new PersistConfig("/test", 1, true, status.getUfsPath())));
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    Assert.assertTrue(ufs.exists(ufsPath));

    // run the persist job again with the overwrite flag and check that it succeeds
    waitForJobToFinish(mJobMaster.run(new PersistConfig("/test", 1, true, status.getUfsPath())));
    Assert.assertTrue(ufs.exists(ufsPath));

    // run the persist job again without the overwrite flag and check it fails
    final long jobId = mJobMaster.run(new PersistConfig("/test", 1, false, status.getUfsPath()));
    waitForJobFailure(jobId);
  }

  /**
   * Tests persisting a file.
   */
  @Test
  public void persistWithAccessTimeUnchangedTest() throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    // run the persist job and check that it succeeds
    waitForJobToFinish(mJobMaster.run(new PersistConfig("/test", 1, true, status.getUfsPath())));
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    Assert.assertTrue(ufs.exists(ufsPath));

    // check file access time is not changed
    URIStatus newStatus = mFileSystem.getStatus(filePath);
    Assert.assertEquals(newStatus.getLastAccessTimeMs(), status.getLastAccessTimeMs());
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS, "1ms",
          PropertyKey.Name.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "50ms",
          PropertyKey.Name.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "50ms"})
  public void persistTimeoutTest() throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    // kill job worker
    mLocalAlluxioJobCluster.getWorker().stop();
    // persist the file
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      client.get().scheduleAsyncPersist(new AlluxioURI(TEST_URI),
          FileSystemOptions.scheduleAsyncPersistDefaults(ServerConfiguration.global()));
    }
    CommonUtils.waitFor("persist timeout", () -> {
      try {
        return PersistenceState.NOT_PERSISTED.toString().equals(
            mFileSystem.getStatus(filePath).getPersistenceState());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000));
    // verify timeout and reverted to not persisted
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    // restart master
    mLocalAlluxioClusterResource.get().restartMasters();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient(); // need new client after restart
    // verify not persisted
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    Assert.assertFalse(ufs.exists(ufsPath));
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, "10s"})
  public void disallowIncompletePersist() throws Exception {
    AlluxioURI path = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));

    // Create file, but do not complete
    FileOutStream os = mFileSystem.createFile(path,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE)
            .setMode(TEST_MODE.toProto()).build());

    // schedule an async persist
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      client.get().scheduleAsyncPersist(path,
          FileSystemOptions.scheduleAsyncPersistDefaults(ServerConfiguration.global()));
      Assert.fail("Should not be able to schedule persistence for incomplete file");
    } catch (Exception e) {
      // expected
      Assert.assertTrue("Failure expected to be about incomplete files",
          e.getMessage().toLowerCase().contains("incomplete"));
    }
  }

  @Test(timeout = 30000)
  public void persistOnlyCompleteFiles() throws Exception {
    AlluxioURI path = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));

    // Create file, but do not complete
    FileOutStream os = mFileSystem.createFile(path,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE)
            .setMode(TEST_MODE.toProto()).build());
    URIStatus status = mFileSystem.getStatus(path);

    // Generate a temporary path to be used by the persist job.
    String tempUfsPath =
        PathUtils.temporaryFileName(System.currentTimeMillis(), status.getUfsPath());
    JobMaster jobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    // Run persist job on incomplete file (expected to fail)
    long failId =
        jobMaster.run(new PersistConfig(path.toString(), status.getMountId(), false, tempUfsPath));

    CommonUtils.waitFor("Wait for persist job to complete", () -> {
      try {
        JobInfo jobInfo = jobMaster.getStatus(failId);
        Assert.assertNotEquals("Persist should not succeed for incomplete file", COMPLETED,
            jobInfo.getStatus());
        if (jobInfo.getStatus() == FAILED) {
          // failed job is expected
          Assert.assertTrue("Failure expected to be about incomplete files",
              jobInfo.getErrorMessage().toLowerCase().contains("incomplete"));
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS)
        .setInterval(100));

    // close the file to allow persist to happen
    os.close();

    // Run persist job on complete file (expected to succeed)
    long successId =
        jobMaster.run(new PersistConfig(path.toString(), status.getMountId(), false, tempUfsPath));

    CommonUtils.waitFor("Wait for persist job to complete", () -> {
      try {
        JobInfo jobInfo = jobMaster.getStatus(successId);
        Assert.assertNotEquals("Persist should not fail", FAILED, jobInfo.getStatus());
        return jobInfo.getStatus() == COMPLETED;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS)
        .setInterval(100));
  }
}
