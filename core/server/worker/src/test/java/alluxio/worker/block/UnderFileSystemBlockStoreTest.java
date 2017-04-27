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

import alluxio.thrift.LockBlockTOptions;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.options.OpenUfsBlockOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public final class UnderFileSystemBlockStoreTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long BLOCK_ID = 2;

  private BlockStore mAlluxioBlockStore;
  private OpenUfsBlockOptions mOpenUfsBlockOptions;
  private UfsManager mUfsManager;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mAlluxioBlockStore = Mockito.mock(BlockStore.class);
    mUfsManager = Mockito.mock(UfsManager.class);
    LockBlockTOptions options = new LockBlockTOptions();
    options.setMaxUfsReadConcurrency(5);
    options.setBlockSize(TEST_BLOCK_SIZE);
    options.setOffset(TEST_BLOCK_SIZE);
    options.setUfsPath(mFolder.newFile().getAbsolutePath());
    mOpenUfsBlockOptions = new OpenUfsBlockOptions(options);
  }

  @Test
  public void acquireAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(blockStore.acquireAccess(i + 1, BLOCK_ID, mOpenUfsBlockOptions));
    }

    Assert.assertFalse(blockStore.acquireAccess(6, BLOCK_ID, mOpenUfsBlockOptions));
  }

  @Test
  public void releaseAccess() throws Exception {
    UnderFileSystemBlockStore blockStore =
        new UnderFileSystemBlockStore(mAlluxioBlockStore, mUfsManager);
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(blockStore.acquireAccess(i + 1, BLOCK_ID, mOpenUfsBlockOptions));
      blockStore.releaseAccess(i + 1, BLOCK_ID);
    }

    Assert.assertTrue(blockStore.acquireAccess(6, BLOCK_ID, mOpenUfsBlockOptions));
  }
}
