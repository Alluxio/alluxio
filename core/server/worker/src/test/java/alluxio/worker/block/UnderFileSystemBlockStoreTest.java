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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public final class UnderFileSystemBlockStoreTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long BLOCK_ID = 2;

  private BlockStore mAlluxioBlockStore;
  private Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private UfsManager mUfsManager;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mAlluxioBlockStore = Mockito.mock(BlockStore.class);
    mUfsManager = Mockito.mock(UfsManager.class);
    mOpenUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder().setMaxUfsReadConcurrency(5)
        .setBlockSize(TEST_BLOCK_SIZE).setOffsetInFile(TEST_BLOCK_SIZE)
        .setUfsPath(mFolder.newFile().getAbsolutePath()).build();
  }

  @Test
  public void acquireAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    for (int i = 0; i < 5; i++) {
      assertTrue(blockStore.acquireAccess(i + 1, BLOCK_ID, mOpenUfsBlockOptions));
    }

    assertFalse(blockStore.acquireAccess(6, BLOCK_ID, mOpenUfsBlockOptions));
  }

  @Test
  public void releaseAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    for (int i = 0; i < 5; i++) {
      assertTrue(blockStore.acquireAccess(i + 1, BLOCK_ID, mOpenUfsBlockOptions));
      blockStore.releaseAccess(i + 1, BLOCK_ID);
    }

    assertTrue(blockStore.acquireAccess(6, BLOCK_ID, mOpenUfsBlockOptions));
  }
}
