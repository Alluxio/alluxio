/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.block;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;

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
import org.powermock.reflect.Whitebox;

import tachyon.client.BlockMasterClient;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.NetAddress;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerClient;

/**
 * Tests for {@link TachyonBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockStoreContext.class, NetworkAddressUtils.class,
    WorkerClient.class})
public final class TachyonBlockStoreTest {
  private static final long BLOCK_ID = 3L;
  private static final long BLOCK_LENGTH = 1000L;
  private static final long WORKER_ID_LOCAL = 5L;
  private static final long WORKER_ID_REMOTE = 6L;
  private static final String WORKER_HOSTNAME_LOCAL = "localhost";
  private static final String WORKER_HOSTNAME_REMOTE = "remote";
  private static final int WORKER_RPC_PORT = 7;
  private static final int WORKER_DATA_PORT = 9;
  private static final NetAddress WORKER_NET_ADDRESS_LOCAL =
      new NetAddress(WORKER_HOSTNAME_LOCAL, WORKER_RPC_PORT, WORKER_DATA_PORT);
  private static final NetAddress WORKER_NET_ADDRESS_REMOTE =
      new NetAddress(WORKER_HOSTNAME_REMOTE, WORKER_RPC_PORT, WORKER_DATA_PORT);
  private static final String STORAGE_TIER = "mem";
  private static final BlockLocation BLOCK_LOCATION_LOCAL =
      new BlockLocation(WORKER_ID_LOCAL, WORKER_NET_ADDRESS_LOCAL, STORAGE_TIER);
  private static final BlockLocation BLOCK_LOCATION_REMOTE =
      new BlockLocation(WORKER_ID_REMOTE, WORKER_NET_ADDRESS_REMOTE, STORAGE_TIER);
  /** {@link BlockInfo} representing a block stored both remotely and locally. */
  private static final BlockInfo BLOCK_INFO = new BlockInfo(BLOCK_ID, BLOCK_LENGTH,
      Arrays.asList(BLOCK_LOCATION_REMOTE, BLOCK_LOCATION_LOCAL));

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private File mTestFile;
  private TachyonBlockStore mBlockStore;
  private BlockStoreContext mBlockStoreContext;
  private BlockMasterClient mMasterClient;
  private WorkerClient mWorkerClient;

  /**
   * Sets up a testable {@link TachyonBlockStore}. Setup consists of the following:
   *
   * 1. The singleton {@link BlockStoreContext} is replaced with mBlockStoreContext<br>
   * 2. mBlockStoreContext will return mMasterClient and mWorkerClient when asked for master/worker
   * clients<br>
   * 3. mTestFile is created inside mTestFolder<br>
   * 4. mWorkerClient is made to understand that locking BLOCK_ID should return the path to
   * mTestFile.
   */
  @Before
  public void before() throws Exception {
    mTestFile = mTestFolder.newFile("testFile");

    ClientContext.reset();
    mBlockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    Whitebox.setInternalState(BlockStoreContext.class, "INSTANCE", mBlockStoreContext);
    mBlockStore = TachyonBlockStore.get();
    Whitebox.setInternalState(mBlockStore, "mContext", mBlockStoreContext);

    mMasterClient = PowerMockito.mock(BlockMasterClient.class);
    Mockito.when(mBlockStoreContext.acquireMasterClient()).thenReturn(mMasterClient);

    mWorkerClient = PowerMockito.mock(WorkerClient.class);
    Mockito.when(mWorkerClient.lockBlock(BLOCK_ID)).thenReturn(mTestFile.getAbsolutePath());
    Mockito.when(mBlockStoreContext.acquireWorkerClient(Mockito.anyString()))
        .thenReturn(mWorkerClient);
  }

  /**
   * Tests getInStream when a local block exists, making sure that the local block is preferred.
   */
  @Test
  public void testGetInStreamLocal() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(BLOCK_INFO);
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    Mockito.when(NetworkAddressUtils.getLocalHostName(Mockito.<TachyonConf>any()))
        .thenReturn(WORKER_HOSTNAME_LOCAL);
    BufferedBlockInStream stream = mBlockStore.getInStream(BLOCK_ID);

    Assert.assertTrue(stream instanceof LocalBlockInStream);
    Assert.assertEquals((Long) BLOCK_ID, (Long) Whitebox.getInternalState(stream, "mBlockId"));
    Assert.assertEquals((Long) BLOCK_LENGTH,
        (Long) Whitebox.getInternalState(stream, "mBlockSize"));
    Mockito.verify(mBlockStoreContext).acquireMasterClient();
    Mockito.verify(mBlockStoreContext).releaseMasterClient(mMasterClient);
  }

  /**
   * Tests getInStream when no local block exists, making sure that the first {@link BlockLocation}
   * in the {@link BlockInfo} list is chosen.
   */
  @Test
  public void testGetInStreamRemote() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(BLOCK_INFO);
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    Mockito.when(NetworkAddressUtils.getLocalHostName(Mockito.<TachyonConf>any()))
        .thenReturn(WORKER_HOSTNAME_LOCAL + "_different");
    BufferedBlockInStream stream = mBlockStore.getInStream(BLOCK_ID);

    Assert.assertTrue(stream instanceof RemoteBlockInStream);
    Assert.assertEquals((Long) BLOCK_ID, (Long) Whitebox.getInternalState(stream, "mBlockId"));
    Assert.assertEquals((Long) BLOCK_LENGTH,
        (Long) Whitebox.getInternalState(stream, "mBlockSize"));
    Assert.assertEquals(new InetSocketAddress(WORKER_HOSTNAME_REMOTE, WORKER_DATA_PORT),
        Whitebox.getInternalState(stream, "mLocation"));
    Mockito.verify(mBlockStoreContext).acquireMasterClient();
    Mockito.verify(mBlockStoreContext).releaseMasterClient(mMasterClient);
  }
}
