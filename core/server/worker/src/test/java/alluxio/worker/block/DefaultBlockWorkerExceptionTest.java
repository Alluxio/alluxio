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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.hdfs.AlluxioHdfsException;
import alluxio.underfs.options.OpenOptions;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test ufs exception handling for {@link DefaultBlockWorker}.
 */
public class DefaultBlockWorkerExceptionTest {
  private static final String FILE_NAME = "/test";
  private static final int BLOCK_SIZE = 128;
  private UnderFileSystem mUfs;
  private DefaultBlockWorker mBlockWorker;
  private final String mRootUfs = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  private final String mMemDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(
      new ImmutableMap.Builder<PropertyKey, Object>().put(PropertyKey.WORKER_TIERED_STORE_LEVELS, 1)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir)
          .put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              AlluxioTestDirectory.createTemporaryDirectory("DefaultBlockWorkerTest")
                  .getAbsolutePath()).build(), Configuration.modifiableGlobal());

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    BlockMasterClient blockMasterClient = mock(BlockMasterClient.class);
    BlockMasterClientPool blockMasterClientPool = spy(new BlockMasterClientPool());
    when(blockMasterClientPool.createNewResource()).thenReturn(blockMasterClient);
    TieredBlockStore tieredBlockStore = spy(new TieredBlockStore());
    UfsManager ufsManager = mock(UfsManager.class);
    AtomicReference<Long> workerId = new AtomicReference<>(-1L);
    BlockStore blockstore =
        new MonoBlockStore(tieredBlockStore, blockMasterClientPool, ufsManager, workerId);
    FileSystemMasterClient client = mock(FileSystemMasterClient.class);
    Sessions sessions = mock(Sessions.class);
    mUfs = mock(UnderFileSystem.class);
    UfsManager.UfsClient ufsClient = new UfsManager.UfsClient(() -> mUfs, new AlluxioURI(mRootUfs));
    when(ufsManager.get(anyLong())).thenReturn(ufsClient);
    mBlockWorker =
        new DefaultBlockWorker(blockMasterClientPool, client, sessions, blockstore, workerId);
  }

  @Test
  public void loadFailure() throws IOException, ExecutionException, InterruptedException {
    AlluxioHdfsException exception = AlluxioHdfsException.fromUfsException(new IOException());
    when(mUfs.openExistingFile(eq(FILE_NAME), any(OpenOptions.class))).thenThrow(exception,
        new RuntimeException(), new IOException());
    int blockId = 0;
    Block blocks = Block.newBuilder().setBlockId(blockId).setLength(BLOCK_SIZE).setMountId(0)
        .setOffsetInFile(0).setUfsPath(FILE_NAME).build();
    List<BlockStatus> failure =
        mBlockWorker.load(Collections.singletonList(blocks), "test", OptionalLong.empty()).get();
    assertEquals(failure.size(), 1);
    assertEquals(exception.getStatus().getCode().value(), failure.get(0).getCode());
    failure =
        mBlockWorker.load(Collections.singletonList(blocks), "test", OptionalLong.empty()).get();
    assertEquals(failure.size(), 1);
    assertEquals(2, failure.get(0).getCode());
    failure =
        mBlockWorker.load(Collections.singletonList(blocks), "test", OptionalLong.empty()).get();
    assertEquals(failure.size(), 1);
    assertEquals(2, failure.get(0).getCode());
  }

  @Test
  public void loadTimeout() throws IOException, ExecutionException, InterruptedException {
    when(mUfs.openExistingFile(eq(FILE_NAME), any(OpenOptions.class))).thenAnswer(
        (Answer<String>) invocationOnMock -> {
          Thread.sleep(Configuration.getMs(PropertyKey.USER_NETWORK_RPC_KEEPALIVE_TIMEOUT) * 2);
          return null;
        });
    int blockId = 0;
    Block blocks = Block.newBuilder().setBlockId(blockId).setLength(BLOCK_SIZE).setMountId(0)
        .setOffsetInFile(0).setUfsPath(FILE_NAME).build();
    List<BlockStatus> failure =
        mBlockWorker.load(Collections.singletonList(blocks), "test", OptionalLong.empty()).get();
    assertEquals(failure.size(), 1);
    assertEquals(Status.DEADLINE_EXCEEDED.getCode().value(), failure.get(0).getCode());
  }
}
