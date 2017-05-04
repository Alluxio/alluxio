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

import alluxio.client.WriteType;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.resource.LockBlockResource;
import alluxio.exception.PreconditionMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.resource.DummyCloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tests for {@link AlluxioBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public final class AlluxioBlockStoreTest {
  private static final long BLOCK_ID = 3L;
  private static final long BLOCK_LENGTH = 100L;
  private static final long LOCK_ID = 44L;
  private static final String WORKER_HOSTNAME_LOCAL = NetworkAddressUtils.getLocalHostName();
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
   * A mock class used to return controlled result when selecting workers.
   */
  @ThreadSafe
  private static class MockFileWriteLocationPolicy implements FileWriteLocationPolicy {
    private final List<WorkerNetAddress> mWorkerNetAddresses;
    private int mIndex;

    /**
     * Constructs this mock policy that returns the given result, once a time, in the input order.
     *
     * @param addresses list of addresses this mock policy will return
     */
    public MockFileWriteLocationPolicy(List<WorkerNetAddress> addresses) {
      mWorkerNetAddresses = Lists.newArrayList(addresses);
      mIndex = 0;
    }

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
        long blockSizeBytes) {
      return mWorkerNetAddresses.get(mIndex++);
    }
  }

  /**
   * The rule for a temporary folder.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private BlockMasterClient mMasterClient;
  private BlockWorkerClient mBlockWorkerClient;
  private AlluxioBlockStore mBlockStore;
  private Channel mChannel;
  private ChannelPipeline mPipeline;
  private InetSocketAddress mLocalAddr;
  private FileSystemContext mContext;

  @Before
  public void before() throws Exception {
    mBlockWorkerClient = PowerMockito.mock(BlockWorkerClient.class);
    mMasterClient = PowerMockito.mock(BlockMasterClient.class);
    mChannel = PowerMockito.mock(Channel.class);
    mPipeline = PowerMockito.mock(ChannelPipeline.class);

    mContext = PowerMockito.mock(FileSystemContext.class);
    // Mock block store context to return our mock clients
    Mockito.when(mContext.createBlockWorkerClient(Mockito.any(WorkerNetAddress.class)))
        .thenReturn(mBlockWorkerClient);
    mLocalAddr = new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0);
    Mockito.when(mBlockWorkerClient.getDataServerAddress()).thenReturn(mLocalAddr);

    Mockito.when(mContext.acquireBlockMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(mMasterClient));

    mBlockStore = new AlluxioBlockStore(mContext, WORKER_HOSTNAME_LOCAL);

    Mockito.when(mContext.acquireNettyChannel(Mockito.any(InetSocketAddress.class)))
        .thenReturn(mChannel);
    Mockito.when(mChannel.pipeline()).thenReturn(mPipeline);
    Mockito.when(mPipeline.last()).thenReturn(new RPCMessageDecoder());
    Mockito.when(mPipeline.addLast(Mockito.any(ChannelHandler.class))).thenReturn(mPipeline);
  }

  /**
   * Tests {@link AlluxioBlockStore#getInStream(long, InStreamOptions)} when a local block
   * exists, making sure that the local block is preferred.
   */
  @Test
  public void getInStreamLocal() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo()
        .setLocations(Arrays.asList(BLOCK_LOCATION_REMOTE, BLOCK_LOCATION_LOCAL)));

    File mTestFile = mTestFolder.newFile("testFile");
    // When a block lock for id BLOCK_ID is requested, a path to a temporary file is returned
    Mockito.when(mBlockWorkerClient.lockBlock(BLOCK_ID, LockBlockOptions.defaults())).thenReturn(
        new LockBlockResource(mBlockWorkerClient,
            new LockBlockResult().setLockId(LOCK_ID).setBlockPath(mTestFile.getAbsolutePath()),
            BLOCK_ID));

    InputStream stream = mBlockStore.getInStream(BLOCK_ID, InStreamOptions.defaults());
    Assert.assertEquals(alluxio.client.block.stream.BlockInStream.class, stream.getClass());
  }

  /**
   * Tests {@link AlluxioBlockStore#getInStream(long, InStreamOptions)} when no local block
   * exists, making sure that the first {@link BlockLocation} in the {@link BlockInfo} list is
   * chosen.
   */
  @Test
  public void getInStreamRemote() throws Exception {
    Mockito.when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo()
        .setLocations(Arrays.asList(BLOCK_LOCATION_REMOTE)));

    File mTestFile = mTestFolder.newFile("testFile");
    // When a block lock for id BLOCK_ID is requested, a path to a temporary file is returned
    Mockito.when(mBlockWorkerClient.lockBlock(BLOCK_ID, LockBlockOptions.defaults())).thenReturn(
        new LockBlockResource(mBlockWorkerClient,
            new LockBlockResult().setLockId(LOCK_ID).setBlockPath(mTestFile.getAbsolutePath()),
            BLOCK_ID));

    InputStream stream = mBlockStore.getInStream(BLOCK_ID, InStreamOptions.defaults());
    Assert.assertEquals(alluxio.client.block.stream.BlockInStream.class, stream.getClass());
  }

  @Test
  public void getOutStreamUsingLocationPolicy() throws Exception {
    OutStreamOptions options = OutStreamOptions.defaults().setWriteType(WriteType.MUST_CACHE)
        .setLocationPolicy(new FileWriteLocationPolicy() {
          @Override
          public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
              long blockSizeBytes) {
            throw new RuntimeException("policy threw exception");
          }
        });
    try {
      mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
      Assert.fail("An exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals("policy threw exception", e.getMessage());
    }
  }

  @Test
  public void getOutStreamMissingLocationPolicy() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE).setLocationPolicy(null);
    try {
      mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
      Assert.fail("missing location policy should fail");
    } catch (NullPointerException e) {
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString(),
          e.getMessage());
    }
  }

  @Test
  public void getOutStreamLocal() throws Exception {
    File tmp = mTestFolder.newFile();
    Mockito.when(mBlockWorkerClient
        .requestBlockLocation(Matchers.eq(BLOCK_ID), Matchers.anyLong(), Matchers.anyInt()))
        .thenReturn(tmp.getAbsolutePath());

    OutStreamOptions options = OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_LOCAL)))
        .setWriteType(WriteType.MUST_CACHE);
    OutputStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    Assert.assertEquals(alluxio.client.block.stream.BlockOutStream.class, stream.getClass());
  }

  @Test
  public void getOutStreamRemote() throws Exception {
    OutStreamOptions options = OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_REMOTE)))
        .setWriteType(WriteType.MUST_CACHE);
    OutputStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    Assert.assertEquals(alluxio.client.block.stream.BlockOutStream.class, stream.getClass());
  }
}
