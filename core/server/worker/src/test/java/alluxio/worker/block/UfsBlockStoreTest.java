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

import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.thrift.LockBlockTOptions;
import alluxio.worker.block.meta.UfsBlockMeta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public final class UfsBlockStoreTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long BLOCK_ID = 2;

  private BlockStore mAlluxioBlockStore;
  private LockBlockTOptions mLockBlockTOptions;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mAlluxioBlockStore = Mockito.mock(BlockStore.class);

    LockBlockTOptions options = new LockBlockTOptions();
    options.setMaxUfsReadConcurrency(10);
    options.setBlockSize(TEST_BLOCK_SIZE);
    options.setOffset(TEST_BLOCK_SIZE);
    options.setUfsPath(mFolder.newFile().getAbsolutePath());
    mLockBlockTOptions = options;
  }

  @Test
  public void acquireAccess() throws Exception {
    UfsBlockStore blockStore = new UfsBlockStore(mAlluxioBlockStore);
    for (int i = 0; i < 5; i++) {
      UfsBlockMeta.ConstMeta constMeta =
          new UfsBlockMeta.ConstMeta(i + 1, BLOCK_ID, mLockBlockTOptions);
      blockStore.acquireAccess(constMeta, 5);
    }

    try {
      UfsBlockMeta.ConstMeta constMeta =
          new UfsBlockMeta.ConstMeta(6, BLOCK_ID, mLockBlockTOptions);
      blockStore.acquireAccess(constMeta, 5);
      Assert.fail();
    } catch (UfsBlockAccessTokenUnavailableException e) {
      // expected
    }
  }

  @Test
  public void releaseAccess() throws Exception {
    UfsBlockStore blockStore = new UfsBlockStore(mAlluxioBlockStore);
    for (int i = 0; i < 5; i++) {
      UfsBlockMeta.ConstMeta constMeta =
          new UfsBlockMeta.ConstMeta(i + 1, BLOCK_ID, mLockBlockTOptions);
      blockStore.acquireAccess(constMeta, 5);
      blockStore.releaseAccess(constMeta.mSessionId, constMeta.mBlockId);
    }

    UfsBlockMeta.ConstMeta constMeta = new UfsBlockMeta.ConstMeta(6, BLOCK_ID, mLockBlockTOptions);
    blockStore.acquireAccess(constMeta, 5);
  }
}
