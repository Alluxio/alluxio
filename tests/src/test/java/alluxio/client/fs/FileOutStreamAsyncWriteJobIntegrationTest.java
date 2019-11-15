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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.Mode;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.PersistenceTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ModeUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockWorker;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for {@link FileOutStream} of under storage type being async persist.
 */
public final class FileOutStreamAsyncWriteJobIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {
  private static final int LEN = 1024;
  private static final FileSystemMasterCommonPOptions COMMON_OPTIONS =
      FileSystemMasterCommonPOptions.newBuilder()
          .setTtl(12345678L).setTtlAction(TtlAction.DELETE)
          .setSyncIntervalMs(-1)
          .build();

  private static final SetAttributePOptions TEST_OPTIONS =
      SetAttributePOptions.newBuilder().setMode(new Mode((short) 0555).toProto())
          .setCommonOptions(COMMON_OPTIONS)
          .build();

  private AlluxioURI mUri;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    mUri = new AlluxioURI(PathUtils.uniqPath());
    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
  }

  /**
   * Helper function to create a file of length LEN with {@link WriteType#ASYNC_THROUGH}.
   *
   * @return ths URIStatus of this file after creation
   */
  private URIStatus createAsyncFile() throws Exception {
    FileOutStreamTestUtils.writeIncreasingByteArrayToFile(mFileSystem, mUri, LEN,
        CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build());
    return mFileSystem.getStatus(mUri);
  }

  @Test
  public void simpleDurableWrite() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);

    URIStatus status = createAsyncFile();
    // check the file is completed but not persisted
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
  }

  @Test
  public void exists() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    Assert.assertTrue(mFileSystem.exists(mUri));

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    Assert.assertTrue(mFileSystem.exists(mUri));

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    Assert.assertTrue(mFileSystem.exists(mUri));
  }

  @Test
  public void deleteBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(sLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void deleteAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(sLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void deleteAfterPersist() throws Exception {
    URIStatus status = createAsyncFile();
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void freeBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    createAsyncFile();
    try {
      mFileSystem.free(mUri);
      Assert.fail("Expect free to fail before file is persisted");
    } catch (AlluxioException e) {
      // Expected
    }
    IntegrationTestUtils.waitForBlocksToBeFreed(
        sLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class));
    URIStatus status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
  }

  @Test
  public void freeAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    try {
      mFileSystem.free(mUri);
      Assert.fail("Expect free to fail before file is persisted");
    } catch (AlluxioException e) {
      // Expected
    }
    IntegrationTestUtils.waitForBlocksToBeFreed(
        sLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class));
    status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
    status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());
  }

  @Test
  public void freeAfterFilePersisted() throws Exception {
    URIStatus status = createAsyncFile();
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    mFileSystem.free(mUri);
    IntegrationTestUtils.waitForBlocksToBeFreed(
        sLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class),
        status.getBlockIds().toArray(new Long[status.getBlockIds().size()]));
    status = mFileSystem.getStatus(mUri);
    // file persisted, free is no more a no-op
    Assert.assertEquals(0, status.getInMemoryPercentage());
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
  }

  @Test
  public void getStatus() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    URIStatus statusAfter = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), statusAfter.getPersistenceState());
  }

  @Test
  public void openFile() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
  }

  @Test
  public void renameBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
    checkFileNotInUnderStorage(mFileSystem.getStatus(newUri).getUfsPath());

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, newUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void renameAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
    checkFileNotInUnderStorage(mFileSystem.getStatus(newUri).getUfsPath());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, newUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void renameAfterFilePersisted() throws Exception {
    URIStatus status = createAsyncFile();
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void setAttributeBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());
    // Skip checking mode for object stores
    Assume.assumeFalse(ufs.isObjectStorage());
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()),
        ufs.getFileStatus(ufsPath).getMode());
  }

  @Test
  public void setAttributeAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());
    // Skip checking mode for object stores
    Assume.assumeFalse(ufs.isObjectStorage());
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()),
        ufs.getFileStatus(ufsPath).getMode());
  }

  @Test
  public void setAttributeAfterFilePersisted() throws Exception {
    createAsyncFile();
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, mUri);
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, mUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, mUri, LEN);
    URIStatus status = mFileSystem.getStatus(mUri);
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());
    // Skip checking mode for object stores
    Assume.assumeFalse(ufs.isObjectStorage());
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()),
        ufs.getFileStatus(ufsPath).getMode());
  }

  @Test
  public void renameScheduleRename() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri1 = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri1.getParent());
    mFileSystem.rename(mUri, newUri1);
    String ufsPath1 = mFileSystem.getStatus(newUri1).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri1, LEN);
    checkFileNotInUnderStorage(ufsPath1);

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    AlluxioURI newUri2 = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.rename(newUri1, newUri2);
    String ufsPath2 = mFileSystem.getStatus(newUri2).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri1);
    checkFileNotInUnderStorage(ufsPath1);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri2, LEN);
    checkFileNotInUnderStorage(ufsPath2);

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, newUri2);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri1);
    checkFileNotInUnderStorage(ufsPath1);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri2, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, newUri2, LEN);
  }

  @Test
  public void renameScheduleFree() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    try {
      mFileSystem.free(newUri);
      Assert.fail("Expect free to fail before file is persisted");
    } catch (AlluxioException e) {
      // Expected
    }
    IntegrationTestUtils.waitForBlocksToBeFreed(
        sLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class));
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, mFileSystem.getStatus(newUri).getInMemoryPercentage());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, newUri, LEN);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, mFileSystem.getStatus(newUri).getInMemoryPercentage());
  }

  @Test
  public void renameScheduleSetAttribute() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.setAttribute(newUri, TEST_OPTIONS);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);
    status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    FileOutStreamTestUtils.checkFileInUnderStorage(mFileSystem, newUri, LEN);
    status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());
    // Skip checking mode for object stores
    Assume.assumeFalse(ufs.isObjectStorage());
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()),
        ufs.getFileStatus(newUfsPath).getMode());
  }

  @Test
  public void renameScheduleDelete() throws Exception {
    PersistenceTestUtils.pauseScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(sLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    FileOutStreamTestUtils.checkFileInAlluxio(mFileSystem, newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(sLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.delete(newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeChecker(sLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(sLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri);
    checkFileNotInUnderStorage(newUfsPath);
  }

  /**
   * Checks the given file exists in Alluxio storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param filePath path of the tmp file
   */
  private void checkFileNotInAlluxio(AlluxioURI filePath) throws Exception {
    Assert.assertFalse(mFileSystem.exists(filePath));
  }

  /**
   * Checks the given file exists in Alluxio storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param ufsPath path of the tmp file
   */
  private void checkFileNotInUnderStorage(String ufsPath) throws Exception {
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    Assert.assertFalse(ufs.exists(ufsPath));
  }
}
