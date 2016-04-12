/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Configuration;
import alluxio.resource.DummyCloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Tests for {@link AlluxioBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockStoreContext.class, NetworkAddressUtils.class,
    BlockWorkerClient.class})
public final class AlluxioBlockStoreTest {
  private static final long BLOCK_ID = 3L;
  private static final long BLOCK_LENGTH = 1000L;
  private static final long LOCK_ID = 44L;
  private static final long WORKER_ID_LOCAL = 5L;
  private static final long WORKER_ID_REMOTE = 6L;
  private static final String WORKER_HOSTNAME_LOCAL = "localhost";
  private static final String WORKER_HOSTNAME_REMOTE = "remote";
  private static final int WORKER_RPC_PORT = 7;
  private static final int WORKER_DATA_PORT = 9;
  private static final int WORKER_WEB_PORT = 10;
  private static final WorkerNetAddress WORKER_NET_ADDRESS_LOCAL = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_LOCAL).setRpcPort(WORKER_RPC_PORT).setDataPort(WORKER_DATA_PORT)
      .setWebPort(WORKER_WEB_PORT);
  private static final WorkerNetAddress WORKER_NET_ADDRESS_REMOTE = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_REMOTE).setRpcPort(WORKER_RPC_PORT).setDataPort(WORKER_DATA_PORT)
      .setWebPort(WORKER_WEB_PORT);
  private static final String STORAGE_TIER = "mem";
  private static final BlockLocation BLOCK_LOCATION_LOCAL = new BlockLocation()
      .setWorkerId(WORKER_ID_LOCAL).setWorkerAddress(WORKER_NET_ADDRESS_LOCAL)
      .setTierAlias(STORAGE_TIER);
  private static final BlockLocation BLOCK_LOCATION_REMOTE = new BlockLocation()
      .setWorkerId(WORKER_ID_REMOTE).setWorkerAddress(WORKER_NET_ADDRESS_REMOTE)
      .setTierAlias(STORAGE_TIER);
  /** {@link BlockInfo} representing a block stored both remotely and locally. */
  private static final BlockInfo BLOCK_INFO = new BlockInfo().setBlockId(BLOCK_ID)
      .setLength(BLOCK_LENGTH)
      .setLocations(Arrays.asList(BLOCK_LOCATION_REMOTE, BLOCK_LOCATION_LOCAL));

  /**
   * The rule for a temporary folder.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private static BlockStoreContext sBlockStoreContext;
  private static AlluxioBlockStore sBlockStore;
  private static BlockMasterClient sMasterClient;
  private static BlockWorkerClient sBlockWorkerClient;

  private File mTestFile;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Replace the singleton BlockStoreContext.INSTANCE with a mock we can control
    sBlockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    Whitebox.setInternalState(BlockStoreContext.class, "INSTANCE", sBlockStoreContext);

    // Mock block store should return our mock clients
    sBlockWorkerClient = PowerMockito.mock(BlockWorkerClient.class);
    Mockito.when(sBlockStoreContext.acquireWorkerClient(Mockito.any(WorkerNetAddress.class)))
        .thenReturn(sBlockWorkerClient);
    sMasterClient = PowerMockito.mock(BlockMasterClient.class);
    Mockito.when(sBlockStoreContext.acquireMasterClientResource()).thenReturn(
        new DummyCloseableResource<BlockMasterClient>(sMasterClient));

    // Inform the block store that it should use the mock context
    sBlockStore = AlluxioBlockStore.get();
    Whitebox.setInternalState(sBlockStore, "mContext", sBlockStoreContext);
  }

  @Before
  public void before() throws Exception {
    mTestFile = mTestFolder.newFile("testFile");
    // When a block lock for id BLOCK_ID is requested, a path to a temporary file is returned
    Mockito.when(sBlockWorkerClient.lockBlock(BLOCK_ID)).thenReturn(
        new LockBlockResult().setLockId(LOCK_ID).setBlockPath(mTestFile.getAbsolutePath()));
  }

  /**
   * Tests {@link AlluxioBlockStore#getInStream(long)} when a local block exists, making sure that
   * the local block is preferred.
   *
   * @throws Exception when getting the reading stream fails
   */
  @Test
  public void getInStreamLocalTest() throws Exception {
    Mockito.when(sMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(BLOCK_INFO);
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    Mockito.when(NetworkAddressUtils.getLocalHostName(Mockito.<Configuration>any()))
        .thenReturn(WORKER_HOSTNAME_LOCAL);
    BufferedBlockInStream stream = sBlockStore.getInStream(BLOCK_ID);

    Assert.assertTrue(stream instanceof LocalBlockInStream);
    Assert.assertEquals(Long.valueOf(BLOCK_ID), Whitebox.getInternalState(stream, "mBlockId"));
    Assert.assertEquals(Long.valueOf(BLOCK_LENGTH),
        Whitebox.getInternalState(stream, "mBlockSize"));
  }

  /**
   * Tests {@link AlluxioBlockStore#getInStream(long)} when no local block exists, making sure that
   * the first {@link BlockLocation} in the {@link BlockInfo} list is chosen.
   *
   * @throws Exception when getting the reading stream fails
   */
  @Test
  public void getInStreamRemoteTest() throws Exception {
    Mockito.when(sMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(BLOCK_INFO);
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    Mockito.when(NetworkAddressUtils.getLocalHostName(Mockito.<Configuration>any()))
        .thenReturn(WORKER_HOSTNAME_LOCAL + "_different");
    BufferedBlockInStream stream = sBlockStore.getInStream(BLOCK_ID);

    Assert.assertTrue(stream instanceof RemoteBlockInStream);
    Assert.assertEquals(Long.valueOf(BLOCK_ID), Whitebox.getInternalState(stream, "mBlockId"));
    Assert.assertEquals(Long.valueOf(BLOCK_LENGTH),
        Whitebox.getInternalState(stream, "mBlockSize"));
    Assert.assertEquals(new InetSocketAddress(WORKER_HOSTNAME_REMOTE, WORKER_DATA_PORT),
        Whitebox.getInternalState(stream, "mWorkerInetSocketAddress"));
  }
}
