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

package alluxio.client.block;

import alluxio.resource.DummyCloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.Arrays;

/**
 * Tests for {@link AlluxioBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockStoreContext.class})
public final class AlluxioBlockStoreTest {
  private static final long BLOCK_ID = 3L;
  private static final long LOCK_ID = 44L;
  private static final String WORKER_HOSTNAME_LOCAL = "localhost";
  private static final String WORKER_HOSTNAME_REMOTE = "remote";
  private static final WorkerNetAddress WORKER_NET_ADDRESS_LOCAL = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_LOCAL);
  private static final WorkerNetAddress WORKER_NET_ADDRESS_REMOTE = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_REMOTE);
  private static final BlockLocation BLOCK_LOCATION_LOCAL = new BlockLocation()
      .setWorkerAddress(WORKER_NET_ADDRESS_LOCAL);
  private static final BlockLocation BLOCK_LOCATION_REMOTE = new BlockLocation()
      .setWorkerAddress(WORKER_NET_ADDRESS_REMOTE);

  /**
   * The rule for a temporary folder.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private BlockMasterClient mMasterClient;
  private BlockWorkerClient mBlockWorkerClient;
  private AlluxioBlockStore mBlockStore;

  @Before
  public void before() throws Exception {
    mBlockWorkerClient = PowerMockito.mock(BlockWorkerClient.class);
    mMasterClient = PowerMockito.mock(BlockMasterClient.class);

    BlockStoreContext blockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    // Mock block store context to return our mock clients
    Mockito.when(blockStoreContext.acquireWorkerClient(Mockito.any(WorkerNetAddress.class)))
        .thenReturn(mBlockWorkerClient);
    Mockito.when(blockStoreContext.acquireMasterClientResource()).thenReturn(
        new DummyCloseableResource<>(mMasterClient));

    mBlockStore = new AlluxioBlockStore(blockStoreContext, WORKER_HOSTNAME_LOCAL);
  }

  /**
   * Tests {@link AlluxioBlockStore#getInStream(long)} when a local block exists, making sure that
   * the local block is preferred.
   */
  @Test
  public void getInStreamLocalTest() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo()
        .setLocations(Arrays.asList(BLOCK_LOCATION_REMOTE, BLOCK_LOCATION_LOCAL)));

    File mTestFile = mTestFolder.newFile("testFile");
    // When a block lock for id BLOCK_ID is requested, a path to a temporary file is returned
    Mockito.when(mBlockWorkerClient.lockBlock(BLOCK_ID)).thenReturn(
        new LockBlockResult().setLockId(LOCK_ID).setBlockPath(mTestFile.getAbsolutePath()));

    BufferedBlockInStream stream = mBlockStore.getInStream(BLOCK_ID);
    Assert.assertEquals(LocalBlockInStream.class, stream.getClass());
  }

  /**
   * Tests {@link AlluxioBlockStore#getInStream(long)} when no local block exists, making sure that
   * the first {@link BlockLocation} in the {@link BlockInfo} list is chosen.
   */
  @Test
  public void getInStreamRemoteTest() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo()
        .setLocations(Arrays.asList(BLOCK_LOCATION_REMOTE)));

    File mTestFile = mTestFolder.newFile("testFile");
    // When a block lock for id BLOCK_ID is requested, a path to a temporary file is returned
    Mockito.when(mBlockWorkerClient.lockBlock(BLOCK_ID)).thenReturn(
        new LockBlockResult().setLockId(LOCK_ID).setBlockPath(mTestFile.getAbsolutePath()));

    BufferedBlockInStream stream = mBlockStore.getInStream(BLOCK_ID);
    Assert.assertEquals(RemoteBlockInStream.class, stream.getClass());
  }
}
