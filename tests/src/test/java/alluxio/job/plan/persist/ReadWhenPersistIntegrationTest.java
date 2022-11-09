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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.file.AlluxioFileInStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.JobIntegrationTest;
import alluxio.master.file.meta.PersistenceState;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Integration tests for {@link AlluxioFileInStream#refreshUriStatusIfNeeded(IOException)}.
 */
public final class ReadWhenPersistIntegrationTest extends JobIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReadWhenPersistIntegrationTest.class);

  private static final String FILE_PATH = "/file";
  private DefaultBlockWorker mBlockWorker;

  @Before
  public void before() throws Exception {
    super.before();
    mBlockWorker = (DefaultBlockWorker) mLocalAlluxioClusterResource.get()
        .getWorkerProcess().getWorker(BlockWorker.class);
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "10s"
  })
  public void testFailIfOpenFileInStreamBeforePersist() throws Exception {
    FileSystemTestUtils.createByteFile(
        mFileSystem, FILE_PATH, WritePType.MUST_CACHE, Constants.KB);

    FileInStream in = mFileSystem.openFile(new AlluxioURI(FILE_PATH));

    // do persist after open
    mFileSystem.persist(new AlluxioURI(FILE_PATH));
    waitForPersisted(mFileSystem, FILE_PATH);

    // simulate that block is evicted
    Long blockId = mFileSystem.getStatus(new AlluxioURI(FILE_PATH)).getBlockIds().get(0);
    mBlockWorker.removeBlock(Sessions.createInternalSessionId(), blockId);

    try {
      in.positionedRead(0, new byte[10], 0, 10);
      fail("should throw UnavailableException: Block .* is unavailable in both Alluxio and "
          + "UFS, or NotFoundException: BlockMeta not found for blockId");
    } catch (UnavailableException | NotFoundException ignored) { /* ignored */ }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "10s"
  })
  public void testSuccessIfOpenFileInStreamBeforePersist() throws Exception {
    FileSystemTestUtils.createByteFile(
        mFileSystem, FILE_PATH, WritePType.MUST_CACHE, Constants.KB);

    FileInStream in = mFileSystem.openFile(new AlluxioURI(FILE_PATH),
        OpenFilePOptions.newBuilder().setUpdateURIStatusWhenRetry(true).build());

    // do persist after open
    mFileSystem.persist(new AlluxioURI(FILE_PATH));
    waitForPersisted(mFileSystem, FILE_PATH);

    // simulate that block is evicted
    Long blockId = mFileSystem.getStatus(new AlluxioURI(FILE_PATH)).getBlockIds().get(0);
    mBlockWorker.removeBlock(Sessions.createInternalSessionId(), blockId);

    assertEquals(10, in.positionedRead(0, new byte[10], 0, 10));
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "10s"
  })
  public void testFailIfOpenFileInStreamWhenPersisting() throws Exception {
    FileSystemTestUtils.createByteFile(
        mFileSystem, FILE_PATH, WritePType.MUST_CACHE, Constants.KB);

    // schedule async persist and open
    mFileSystem.persist(new AlluxioURI(FILE_PATH));
    FileInStream in = mFileSystem.openFile(new AlluxioURI(FILE_PATH));
    if (!PersistenceState.TO_BE_PERSISTED.name().equals(
        mFileSystem.getStatus(new AlluxioURI(FILE_PATH)).getPersistenceState())) {
      LOG.warn("Ineffective test: current status should be TO_BE_PERSISTED");
      return;
    }
    waitForPersisted(mFileSystem, FILE_PATH);

    // simulate that block is evicted
    Long blockId = mFileSystem.getStatus(new AlluxioURI(FILE_PATH)).getBlockIds().get(0);
    mBlockWorker.removeBlock(Sessions.createInternalSessionId(), blockId);

    try {
      assertEquals(10, in.positionedRead(0, new byte[10], 0, 10));
      fail("should throw NotFoundException: Failed to read from UFS");
    } catch (NotFoundException ignored) { /* ignored */ }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "10s"
  })
  public void testSuccessIfOpenFileInStreamWhenPersisting() throws Exception {
    FileSystemTestUtils.createByteFile(
        mFileSystem, FILE_PATH, WritePType.MUST_CACHE, Constants.KB);

    // schedule async persist and open
    mFileSystem.persist(new AlluxioURI(FILE_PATH));
    FileInStream in = mFileSystem.openFile(new AlluxioURI(FILE_PATH),
        OpenFilePOptions.newBuilder().setUpdateURIStatusWhenRetry(true).build());
    waitForPersisted(mFileSystem, FILE_PATH);

    // simulate that block is evicted
    Long blockId = mFileSystem.getStatus(new AlluxioURI(FILE_PATH)).getBlockIds().get(0);
    mBlockWorker.removeBlock(Sessions.createInternalSessionId(), blockId);

    assertEquals(10, in.positionedRead(0, new byte[10], 0, 10));
  }

  private void waitForPersisted(FileSystem fileSystem, String filePath) throws IOException {
    RetryUtils.retry("wait for persisted", () -> {
      try {
        URIStatus status = fileSystem.getStatus(new AlluxioURI(filePath));
        if (!status.isPersisted()) {
          throw new IOException(String.format("%s is not persisted", filePath));
        }
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    }, new ExponentialBackoffRetry(100, 1000, 20));
  }
}
