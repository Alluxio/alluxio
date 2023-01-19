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
import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.MonoBlockStore;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.page.PagedBlockStore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
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
    BlockMasterClientPool blockMasterClientPool = new BlockMasterClientPool();
    AtomicReference<Long> workerId = new AtomicReference<>(-1L);

    return PagedBlockStore.create(ufsManager, blockMasterClientPool, workerId);
  }

  public BlockStoreBase(MonoBlockStore monoBlockStore,
                        PagedBlockStore pagedBlockStore, UfsManager ufs) {
    mMonoBlockStore = monoBlockStore;
    mPagedBlockStore = pagedBlockStore;
    mUfsManager = ufs;
  }

  /**
   * Create a local block to both Mono and Paged block store.
   * Flush some data to the block and commit it for later use.
   *
   * @param blockId block id
   * @param blockSize size of block
   * @param data read-only data to flush to block
   * @throws Exception if I/O error happens
   */
  public void prepareLocalBlock(long blockId, long blockSize, byte[] data)
      throws Exception {
    mMonoBlockStore.createBlock(1, blockId, 0,
                    new CreateBlockOptions(null, null, blockSize));
    try (BlockWriter writer = mMonoBlockStore
            .createBlockWriter(1, blockId)) {
      writer.append(ByteBuffer.wrap(data));
    }
    mMonoBlockStore.commitBlock(1, blockId, false);

    mPagedBlockStore.createBlock(1, blockId, 0,
            new CreateBlockOptions(null, null, blockSize));
    try (BlockWriter writer = mPagedBlockStore.createBlockWriter(1, blockId)) {
      writer.append(ByteBuffer.wrap(data));
    }
    mPagedBlockStore.commitBlock(1, blockId, false);
  }

  /**
   * Mount an ufs to {@link UfsManager}, which is shared for both stores.
   *
   * @param mountId mount id
   * @param rootDir directory path of ufs
   */
  public void mountUfs(long mountId, String rootDir) {
    mUfsManager.addMount(mountId, new AlluxioURI(rootDir),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
  }

  /**
   * Flush some data to the local file located at ufsFilePath.
   *
   * @param ufsFilePath file path
   * @param data ufs file data
   * @throws Exception if error occurs
   */
  public void prepareUfsFile(String ufsFilePath, byte[] data) throws Exception {
    File ufsFile = new File(ufsFilePath);
    if (!ufsFile.createNewFile()) {
      throw new IllegalStateException(String.format("UFS file %s already exists", ufsFilePath));
    }

    try (FileOutputStream out = new FileOutputStream(ufsFile);
         BufferedOutputStream bout = new BufferedOutputStream(out)) {
      bout.write(data);
      bout.flush();
    }
  }

  @Override
  public void close() throws Exception {
    mMonoBlockStore.close();
    mPagedBlockStore.close();
    mUfsManager.close();
    Configuration.reloadProperties();
  }
}
