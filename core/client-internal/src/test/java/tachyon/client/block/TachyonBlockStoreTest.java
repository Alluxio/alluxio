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

import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.LockBlockResult;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Tests for {@link TachyonBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockStoreContext.class, NetworkAddressUtils.class,
    BlockWorkerClient.class})
public final class TachyonBlockStoreTest {
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
  private static final WorkerNetAddress WORKER_NET_ADDRESS_LOCAL = new WorkerNetAddress(
      WORKER_HOSTNAME_LOCAL, WORKER_RPC_PORT, WORKER_DATA_PORT, WORKER_WEB_PORT);
  private static final WorkerNetAddress WORKER_NET_ADDRESS_REMOTE = new WorkerNetAddress(
      WORKER_HOSTNAME_REMOTE, WORKER_RPC_PORT, WORKER_DATA_PORT, WORKER_WEB_PORT);
  private static final String STORAGE_TIER = "mem";
  private static final BlockLocation BLOCK_LOCATION_LOCAL =
      new BlockLocation(WORKER_ID_LOCAL, WORKER_NET_ADDRESS_LOCAL, STORAGE_TIER);
  private static final BlockLocation BLOCK_LOCATION_REMOTE =
      new BlockLocation(WORKER_ID_REMOTE, WORKER_NET_ADDRESS_REMOTE, STORAGE_TIER);
  /** {@link BlockInfo} representing a block stored both remotely and locally. */
  private static final BlockInfo BLOCK_INFO = new BlockInfo(BLOCK_ID, BLOCK_LENGTH,
      Arrays.asList(BLOCK_LOCATION_REMOTE, BLOCK_LOCATION_LOCAL));

  /**
   * The rule for a temporary folder.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private File mTestFile;
  private TachyonBlockStore mBlockStore;
  private BlockStoreContext mBlockStoreContext;
  private BlockMasterClient mMasterClient;
  private BlockWorkerClient mBlockWorkerClient;

  /**
   * Sets up a testable {@link TachyonBlockStore}. Setup consists of the following:
   *
   * 1. The singleton {@link BlockStoreContext} is replaced with {@link #mBlockStoreContext}<br>
   * 2. {@link #mBlockStoreContext} will return {@link #mMasterClient} and
   *    {@link #mBlockWorkerClient} when asked for master/worker clients<br>
   * 3. {@link #mTestFile} is created inside {@link #mTestFolder}<br>
   * 4. {@link #mBlockWorkerClient} is made to understand that locking {@link #BLOCK_ID} should
   *    return the path to {@link #mTestFile}.
   *
   * @throws Exception when acquiring a worker client fails
   */
  @Before
  public void before() throws Exception {
    mTestFile = mTestFolder.newFile("testFile");

    mBlockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    Whitebox.setInternalState(BlockStoreContext.class, "INSTANCE", mBlockStoreContext);
    mBlockStore = TachyonBlockStore.get();
    Whitebox.setInternalState(mBlockStore, "mContext", mBlockStoreContext);

    mMasterClient = PowerMockito.mock(BlockMasterClient.class);
    Mockito.when(mBlockStoreContext.acquireMasterClient()).thenReturn(mMasterClient);

    mBlockWorkerClient = PowerMockito.mock(BlockWorkerClient.class);
    Mockito.when(mBlockWorkerClient.lockBlock(BLOCK_ID)).thenReturn(
        new LockBlockResult(LOCK_ID, mTestFile.getAbsolutePath()));
    Mockito.when(mBlockStoreContext.acquireWorkerClient(Mockito.anyString()))
        .thenReturn(mBlockWorkerClient);
  }

  /**
   * Tests {@link TachyonBlockStore#getInStream(long)} when a local block exists, making sure that
   * the local block is preferred.
   *
   * @throws Exception when getting the reading stream fails
   */
  @Test
  public void getInStreamLocalTest() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(BLOCK_INFO);
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    Mockito.when(NetworkAddressUtils.getLocalHostName(Mockito.<TachyonConf>any()))
        .thenReturn(WORKER_HOSTNAME_LOCAL);
    BufferedBlockInStream stream = mBlockStore.getInStream(BLOCK_ID);

    Assert.assertTrue(stream instanceof LocalBlockInStream);
    Assert.assertEquals(BLOCK_ID, Whitebox.getInternalState(stream, "mBlockId"));
    Assert.assertEquals(BLOCK_LENGTH, Whitebox.getInternalState(stream, "mBlockSize"));
    Mockito.verify(mBlockStoreContext).acquireMasterClient();
    Mockito.verify(mBlockStoreContext).releaseMasterClient(mMasterClient);
  }

  /**
   * Tests {@link TachyonBlockStore#getInStream(long)} when no local block exists, making sure that
   * the first {@link BlockLocation} in the {@link BlockInfo} list is chosen.
   *
   * @throws Exception when getting the reading stream fails
   */
  @Test
  public void getInStreamRemoteTest() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(BLOCK_INFO);
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    Mockito.when(NetworkAddressUtils.getLocalHostName(Mockito.<TachyonConf>any()))
        .thenReturn(WORKER_HOSTNAME_LOCAL + "_different");
    BufferedBlockInStream stream = mBlockStore.getInStream(BLOCK_ID);

    Assert.assertTrue(stream instanceof RemoteBlockInStream);
    Assert.assertEquals(BLOCK_ID, Whitebox.getInternalState(stream, "mBlockId"));
    Assert.assertEquals(BLOCK_LENGTH, Whitebox.getInternalState(stream, "mBlockSize"));
    Assert.assertEquals(new InetSocketAddress(WORKER_HOSTNAME_REMOTE, WORKER_DATA_PORT),
        Whitebox.getInternalState(stream, "mLocation"));
    Mockito.verify(mBlockStoreContext).acquireMasterClient();
    Mockito.verify(mBlockStoreContext).releaseMasterClient(mMasterClient);
  }
}
