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

package alluxio.worker.block;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.master.NoopUfsManager;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.worker.block.io.BlockReader;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

public final class UnderFileSystemBlockStoreTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long BLOCK_ID = 2;
  private static final long MOUNT_ID = 1;

  private LocalBlockStore mAlluxioBlockStore;
  private Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private UfsManager mUfsManager;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();
  private File mTempFile;
  private String mTempFilePath;

  @Before
  public void before() throws Exception {
    mTempFile = mFolder.newFile();
    mTempFilePath = mTempFile.getAbsolutePath();

    mAlluxioBlockStore = Mockito.mock(LocalBlockStore.class);

    // make a mock UfsManager that returns a client connecting
    // to our local file
    mUfsManager = new NoopUfsManager();
    mUfsManager.addMount(
        MOUNT_ID,
        new AlluxioURI(mTempFilePath),
        UnderFileSystemConfiguration.defaults(Configuration.global()));

    mOpenUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder().setMaxUfsReadConcurrency(5)
        .setBlockSize(TEST_BLOCK_SIZE).setOffsetInFile(TEST_BLOCK_SIZE)
        .setUfsPath(mTempFilePath).setMountId(MOUNT_ID).build();
  }

  @Test
  public void acquireAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    for (int i = 0; i < 5; i++) {
      assertTrue(blockStore.acquireAccess(i + 1, BLOCK_ID, mOpenUfsBlockOptions));
    }
  }

  @Test
  public void acquireDuplicateAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    int sessionId = 1;
    assertTrue(blockStore.acquireAccess(sessionId, BLOCK_ID, mOpenUfsBlockOptions));
    assertThrows(
        BlockAlreadyExistsException.class,
        () -> blockStore.acquireAccess(sessionId, BLOCK_ID, mOpenUfsBlockOptions));
  }

  @Test
  public void releaseAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    for (int i = 0; i < 5; i++) {
      assertTrue(blockStore.acquireAccess(i + 1, BLOCK_ID, mOpenUfsBlockOptions));
      blockStore.releaseAccess(i + 1, BLOCK_ID);
    }

    // release non-existing blocks should not throw an exception
    blockStore.releaseAccess(1, BLOCK_ID);

    assertTrue(blockStore.acquireAccess(6, BLOCK_ID, mOpenUfsBlockOptions));
  }

  @Test
  public void createBlockReader() throws Exception {
    // flush some data to test file
    byte[] data = new byte[(int) TEST_BLOCK_SIZE];
    for (int i = 0; i < TEST_BLOCK_SIZE; ++i) {
      data[i] = 1;
    }
    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(mTempFile));
    out.write(data, 0, data.length);
    out.flush();

    // create block metastore that accesses the test file
    long sessionId = 1L;
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions
        .newBuilder()
        .setBlockSize(TEST_BLOCK_SIZE)
        .setMountId(MOUNT_ID)
        .setOffsetInFile(0)
        .setMaxUfsReadConcurrency(1)
        .setUfsPath(mTempFilePath)
        .build();

    BlockReader reader = blockStore
        .createBlockReader(sessionId, BLOCK_ID, 0, false, options);

    assertArrayEquals(data, reader.read(0, TEST_BLOCK_SIZE).array());

    blockStore.close(sessionId, BLOCK_ID);
    blockStore.releaseAccess(sessionId, BLOCK_ID);
  }

  @Test
  public void isNoCache() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    long sessionId = 1L;
    long blockId = 1L;
    Protocol.OpenUfsBlockOptions options =
        mOpenUfsBlockOptions.toBuilder().setNoCache(true).build();

    // we have not acquired this block yet
    assertThrows(
        BlockDoesNotExistRuntimeException.class,
        () -> blockStore.isNoCache(sessionId, blockId));

    blockStore.acquireAccess(sessionId, blockId, options);

    // this should success
    assertTrue(blockStore.isNoCache(sessionId, blockId));
  }

  @Test
  public void cleanUpSession() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    long sessionId = 1L;
    long blockId = 1L;

    blockStore.acquireAccess(sessionId, blockId, mOpenUfsBlockOptions);

    // before clean up, the session-block key is stored
    // and cannot be re-acquired
    assertThrows(
        BlockAlreadyExistsException.class,
        () -> blockStore.acquireAccess(sessionId, blockId, mOpenUfsBlockOptions)
    );

    blockStore.cleanupSession(sessionId);

    // now session is cleaned up, and we can acquire session-block keys
    blockStore.acquireAccess(sessionId, blockId, mOpenUfsBlockOptions);
  }

  @Test
  public void closeReader() throws Exception {
    // create block metastore that accesses the test file
    long sessionId = 1L;
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions
        .newBuilder()
        .setBlockSize(TEST_BLOCK_SIZE)
        .setMountId(MOUNT_ID)
        .setOffsetInFile(0)
        .setMaxUfsReadConcurrency(1)
        .setUfsPath(mTempFilePath)
        .build();

    BlockReader reader = blockStore
        .createBlockReader(sessionId, BLOCK_ID, 0, false, options);

    blockStore.close(sessionId, BLOCK_ID);
    assertTrue(reader.isClosed());
  }
}
