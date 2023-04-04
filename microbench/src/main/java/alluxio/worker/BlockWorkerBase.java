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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.dora.AlluxioTestDirectory;
import alluxio.dora.AlluxioURI;
import alluxio.dora.Constants;
import alluxio.dora.Sessions;
import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.InstancedConfiguration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.underfs.UfsManager;
import alluxio.dora.underfs.UnderFileSystem;
import alluxio.dora.underfs.UnderFileSystemConfiguration;
import alluxio.dora.util.io.BufferUtils;
import alluxio.dora.worker.block.BlockLockManager;
import alluxio.dora.worker.block.BlockMasterClient;
import alluxio.dora.worker.block.BlockMasterClientPool;
import alluxio.dora.worker.block.BlockMetadataManager;
import alluxio.dora.worker.block.DefaultBlockWorker;
import alluxio.dora.worker.block.MonoBlockStore;
import alluxio.dora.worker.block.TieredBlockReaderFactory;
import alluxio.dora.worker.block.TieredBlockStore;
import alluxio.dora.worker.block.TieredBlockWriterFactory;
import alluxio.dora.worker.block.TieredTempBlockMetaFactory;
import alluxio.dora.worker.file.FileSystemMasterClient;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class BlockWorkerBase {

  public final MonoBlockStore mBlockStore;
  private final String mUfs;
  public final DefaultBlockWorker mBlockWorker;

  BlockWorkerBase() throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);
    String memDir =
        AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();
    InstancedConfiguration config = Configuration.modifiableGlobal();
    config.set(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, memDir);
    BlockMasterClient blockMasterClient = mock(BlockMasterClient.class);
    BlockMasterClientPool blockMasterClientPool = spy(new BlockMasterClientPool());
    when(blockMasterClientPool.createNewResource()).thenReturn(blockMasterClient);
    TieredBlockStore tieredBlockStore = new TieredBlockStore(
        BlockMetadataManager.createBlockMetadataManager(),
        new BlockLockManager(),
        new TieredBlockReaderFactory(),
        new TieredBlockWriterFactory(),
        new TieredTempBlockMetaFactory());
    UfsManager ufsManager = mock(UfsManager.class);
    AtomicReference<Long> workerId = new AtomicReference<>(-1L);
    mUfs = AlluxioTestDirectory.createTemporaryDirectory("BlockWorkerBench").getAbsolutePath();
    UfsManager.UfsClient ufsClient = new UfsManager.UfsClient(
        () -> UnderFileSystem.Factory.create(mUfs,
            UnderFileSystemConfiguration.defaults(Configuration.global())), new AlluxioURI(mUfs));
    when(ufsManager.get(anyLong())).thenReturn(ufsClient);
    mBlockStore = new MonoBlockStore(tieredBlockStore, blockMasterClientPool, ufsManager, workerId);
    mBlockStore.initialize();
    FileSystemMasterClient fileSystemMasterClient = mock(FileSystemMasterClient.class);
    Sessions sessions = mock(Sessions.class);
    mBlockWorker =
        new DefaultBlockWorker(blockMasterClientPool, fileSystemMasterClient, sessions, mBlockStore,
            workerId);
  }

  public void after() throws Exception {
    mBlockStore.close();
  }

  public List<String> createFile(int blockCount, int blockSize) throws IOException {
    List<String> ufsFilePath = new ArrayList<>();
    for (int i = 0; i < blockCount; i++) {
      String testFilePath = File.createTempFile("temp", null, new File(mUfs)).getAbsolutePath();
      byte[] buffer = BufferUtils.getIncreasingByteArray(blockSize);
      BufferUtils.writeBufferToFile(testFilePath, buffer);
      ufsFilePath.add(testFilePath);
    }
    return ufsFilePath;
  }
}
