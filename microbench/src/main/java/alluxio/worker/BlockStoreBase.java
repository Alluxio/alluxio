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

package alluxio.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.MonoBlockStore;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.page.PagedBlockStore;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The class contains necessary objects to perform benchmarks on {@link BlockStore}.
 * It is intended for use case of comparing performance of different BlockStore implementations,
 * where only one of the stores will be actually used. For this reason, UFS is shared among all
 * the objects. Users should be careful about this for different kinds of use cases.
 */
public class BlockStoreBase implements AutoCloseable {
  /**
   * {@link MonoBlockStore} backed by {@link TieredBlockStore} as local storage.
   */
  public MonoBlockStore mMonoBlockStore;

  /**
   * {@link PagedBlockStore} that uses page cache as local storage.
   */
  public PagedBlockStore mPagedBlockStore;

  /**
   * The shared {@link UfsManager} used for block stores.
   */
  public UfsManager mUfsManager;

  /**
   * Create a new instance with initialized block store objects.
   *
   * @return the {@link BlockStoreBase}
   */
  public static BlockStoreBase create() {
    UfsManager ufs = new NoopUfsManager();
    MonoBlockStore monoBlockStore = createMonoBlockStore(ufs);
    PagedBlockStore pagedBlockStore = createPagedBlockStore(ufs);
    return new BlockStoreBase(monoBlockStore, pagedBlockStore, ufs);
  }

  private static MonoBlockStore createMonoBlockStore(UfsManager ufsManager) {
    // set up configurations
    Configuration.set(PropertyKey.WORKER_TIERED_STORE_LEVELS, 1);
    String tieredStoreDir = AlluxioTestDirectory
        .createTemporaryDirectory("worker_local_store").getAbsolutePath();
    Configuration.set(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, tieredStoreDir);

    // mock a BlockMasterClientPool as we won't use it
    BlockMasterClient mockClient = mock(BlockMasterClient.class);
    BlockMasterClientPool mockPool = mock(BlockMasterClientPool.class);
    when(mockPool.acquire()).thenReturn(mockClient);

    TieredBlockStore tieredBlockStore = new TieredBlockStore();

    return new MonoBlockStore(tieredBlockStore, mockPool, ufsManager, new AtomicReference<>(1L));
  }

  private static PagedBlockStore createPagedBlockStore(UfsManager ufsManager) {
    // set up configurations
    String cacheDir = AlluxioTestDirectory
        .createTemporaryDirectory("worker_cache").getAbsolutePath();
    Configuration.set(PropertyKey.USER_CLIENT_CACHE_DIRS, cacheDir);

    return PagedBlockStore.create(ufsManager);
  }

  public BlockStoreBase(MonoBlockStore monoBlockStore,
                        PagedBlockStore pagedBlockStore, UfsManager ufs) {
    mMonoBlockStore = monoBlockStore;
    mPagedBlockStore = pagedBlockStore;
    mUfsManager = ufs;
  }

  @Override
  public void close() throws Exception {
    mMonoBlockStore.close();
    mPagedBlockStore.close();
    mUfsManager.close();
    Configuration.reloadProperties();
  }
}
