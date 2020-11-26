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

package alluxio.client.fs.io;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.fs.io.AbstractFileOutStreamIntegrationTest;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.Mode;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.PersistenceTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Assume;
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
  private static final WaitForOptions WAIT_OPTIONS =
      WaitForOptions.defaults().setTimeoutMs(2000).setInterval(10);

  private static final SetAttributePOptions TEST_OPTIONS =
      SetAttributePOptions.newBuilder().setMode(new Mode((short) 0555).toProto())
          .setCommonOptions(COMMON_OPTIONS)
          .build();

  private AlluxioURI mUri = new AlluxioURI(PathUtils.uniqPath());

  /**
   * Helper function to create a file of length LEN with {@link WriteType#ASYNC_THROUGH}.
   *
   * @return ths URIStatus of this file after creation
   */
  private URIStatus createAsyncFile() throws Exception {
    writeIncreasingByteArrayToFile(mUri, LEN, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build());
    return mFileSystem.getStatus(mUri);
  }

  @Test
  public void simpleDurableWrite() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);

    URIStatus status = createAsyncFile();
    // check the file is completed but not persisted
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void exists() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    Assert.assertTrue(mFileSystem.exists(mUri));

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    Assert.assertTrue(mFileSystem.exists(mUri));

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    Assert.assertTrue(mFileSystem.exists(mUri));
  }

  @Test
  public void deleteBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(mLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void deleteAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(mLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void deleteAfterPersist() throws Exception {
    URIStatus status = createAsyncFile();
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void freeBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    createAsyncFile();
    try {
      mFileSystem.free(mUri);
      Assert.fail("Expect free to fail before file is persisted");
    } catch (AlluxioException e) {
      // Expected
    }
    URIStatus status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());
    checkFileInAlluxio(mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void freeAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    try {
      mFileSystem.free(mUri);
      Assert.fail("Expect free to fail before file is persisted");
    } catch (AlluxioException e) {
      // Expected
    }
    status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
    status = mFileSystem.getStatus(mUri);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, status.getInMemoryPercentage());
  }

  @Test
  public void freeAfterFilePersisted() throws Exception {
    URIStatus status = createAsyncFile();
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.free(mUri);
    CommonUtils.waitFor("file is freed", () -> {
      try {
        return 0 == mFileSystem.getStatus(mUri).getInMemoryPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
    status = mFileSystem.getStatus(mUri);
    // file persisted, free is no more a no-op
    Assert.assertEquals(0, status.getInMemoryPercentage());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void getStatus() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    URIStatus statusAfter = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), statusAfter.getPersistenceState());
  }

  @Test
  public void openFile() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    checkFileInAlluxio(mUri, LEN);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    checkFileInAlluxio(mUri, LEN);

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void renameBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
    checkFileNotInUnderStorage(mFileSystem.getStatus(newUri).getUfsPath());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void renameAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
    checkFileNotInUnderStorage(mFileSystem.getStatus(newUri).getUfsPath());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void renameAfterFilePersisted() throws Exception {
    URIStatus status = createAsyncFile();
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void setAttributeBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    checkFileInAlluxio(mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
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
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    checkFileInAlluxio(mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
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
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
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
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri1 = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri1.getParent());
    mFileSystem.rename(mUri, newUri1);
    String ufsPath1 = mFileSystem.getStatus(newUri1).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri1, LEN);
    checkFileNotInUnderStorage(ufsPath1);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    AlluxioURI newUri2 = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.rename(newUri1, newUri2);
    String ufsPath2 = mFileSystem.getStatus(newUri2).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri1);
    checkFileNotInUnderStorage(ufsPath1);
    checkFileInAlluxio(newUri2, LEN);
    checkFileNotInUnderStorage(ufsPath2);

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri2);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri1);
    checkFileNotInUnderStorage(ufsPath1);
    checkFileInAlluxio(newUri2, LEN);
    checkFileInUnderStorage(newUri2, LEN);
  }

  @Test
  public void renameScheduleFree() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    try {
      mFileSystem.free(newUri);
      Assert.fail("Expect free to fail before file is persisted");
    } catch (AlluxioException e) {
      // Expected
    }
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, mFileSystem.getStatus(newUri).getInMemoryPercentage());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    // free for non-persisted file is no-op
    Assert.assertEquals(100, mFileSystem.getStatus(newUri).getInMemoryPercentage());
  }

  @Test
  public void renameScheduleSetAttribute() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.setAttribute(newUri, TEST_OPTIONS);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);
    status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(ModeUtils.protoToShort(TEST_OPTIONS.getMode()), status.getMode());
    Assert.assertEquals(COMMON_OPTIONS.getTtl(), status.getTtl());
    Assert.assertEquals(COMMON_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
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
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.delete(newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(mLocalAlluxioClusterResource, status.getFileId());
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
