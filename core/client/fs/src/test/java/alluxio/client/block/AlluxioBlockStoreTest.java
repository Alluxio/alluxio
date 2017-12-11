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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import alluxio.client.WriteType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.netty.NettyRPC;
import alluxio.client.netty.NettyRPCContext;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.network.TieredIdentityFactory;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions;
import alluxio.resource.DummyCloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tests for {@link AlluxioBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, NettyRPC.class})
public final class AlluxioBlockStoreTest {
  private static final long BLOCK_ID = 3L;
  private static final long BLOCK_LENGTH = 100L;
  private static final String WORKER_HOSTNAME_LOCAL = NetworkAddressUtils.getLocalHostName();
  private static final String WORKER_HOSTNAME_REMOTE = "remote";
  private static final WorkerNetAddress WORKER_NET_ADDRESS_LOCAL = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_LOCAL);
  private static final WorkerNetAddress WORKER_NET_ADDRESS_REMOTE = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_REMOTE);

  /**
   * A mock class used to return controlled result when selecting workers.
   */
  @ThreadSafe
  private static class MockFileWriteLocationPolicy
      implements FileWriteLocationPolicy, BlockLocationPolicy {
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
      if (mWorkerNetAddresses.isEmpty()) {
        return null;
      }
      return mWorkerNetAddresses.get(mIndex++);
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
      return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
    }
  }

  @Rule
  public ExpectedException mException = ExpectedException.none();

  private BlockMasterClient mMasterClient;
  private AlluxioBlockStore mBlockStore;
  private Channel mChannel;
  private ChannelPipeline mPipeline;
  private WorkerNetAddress mLocalAddr;
  private FileSystemContext mContext;

  @Before
  public void before() throws Exception {
    mMasterClient = PowerMockito.mock(BlockMasterClient.class);
    mChannel = PowerMockito.mock(Channel.class);
    mPipeline = PowerMockito.mock(ChannelPipeline.class);

    mContext = PowerMockito.mock(FileSystemContext.class);
    when(mContext.acquireBlockMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(mMasterClient));
    mLocalAddr = new WorkerNetAddress().setHost(NetworkAddressUtils.getLocalHostName());

    mBlockStore = new AlluxioBlockStore(mContext,
        TieredIdentityFactory.fromString("node=" + WORKER_HOSTNAME_LOCAL));

    when(mContext.acquireNettyChannel(Mockito.any(WorkerNetAddress.class)))
        .thenReturn(mChannel);
    when(mChannel.pipeline()).thenReturn(mPipeline);
    when(mPipeline.last()).thenReturn(new RPCMessageDecoder());
    when(mPipeline.addLast(Mockito.any(ChannelHandler.class))).thenReturn(mPipeline);
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
    mException.expect(Exception.class);
    mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
  }

  @Test
  public void getOutStreamMissingLocationPolicy() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE).setLocationPolicy(null);
    mException.expect(NullPointerException.class);
    mException.expectMessage(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString());
    mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
  }

  @Test
  public void getOutStreamNoWorker() throws IOException {
    OutStreamOptions options =
        OutStreamOptions
            .defaults()
            .setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE)
            .setLocationPolicy(
                new MockFileWriteLocationPolicy(Lists.<WorkerNetAddress>newArrayList()));
    mException.expect(UnavailableException.class);
    mException
        .expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(BLOCK_LENGTH));
    mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
  }

  @Test
  public void getOutStreamLocal() throws Exception {
    File file = File.createTempFile("test", ".tmp");
    ProtoMessage message = new ProtoMessage(
        Protocol.LocalBlockCreateResponse.newBuilder().setPath(file.getAbsolutePath()).build());
    PowerMockito.mockStatic(NettyRPC.class);
    when(NettyRPC.call(Mockito.any(NettyRPCContext.class), Mockito.any(ProtoMessage.class)))
        .thenReturn(message);

    OutStreamOptions options = OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_LOCAL)))
        .setWriteType(WriteType.MUST_CACHE);
    BlockOutStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(WORKER_NET_ADDRESS_LOCAL, stream.getAddress());
  }

  @Test
  public void getOutStreamRemote() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1");
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2");
    OutStreamOptions options = OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(Arrays.asList(worker1, worker2)))
        .setWriteType(WriteType.MUST_CACHE);
    BlockOutStream stream1 = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(worker1, stream1.getAddress());
    BlockOutStream stream2 = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(worker2, stream2.getAddress());
  }

  @Test
  public void getInStreamUfs() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1");
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2");
    InStreamOptions options = InStreamOptions.defaults()
        .setUfsReadLocationPolicy(new MockFileWriteLocationPolicy(Arrays.asList(worker1, worker2)));
    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo());
    when(mMasterClient.getWorkerInfoList()).thenReturn(
        Arrays.asList(new WorkerInfo().setAddress(worker1), new WorkerInfo().setAddress(worker2)));

    // Location policy chooses worker1 first.
    assertEquals(worker1, mBlockStore
        .getInStream(BLOCK_ID, OpenUfsBlockOptions.getDefaultInstance(), options).getAddress());
    // Location policy chooses worker2 second.
    assertEquals(worker2, mBlockStore
        .getInStream(BLOCK_ID, OpenUfsBlockOptions.getDefaultInstance(), options).getAddress());
  }

  @Test
  public void getInStreamLocal() throws Exception {
    WorkerNetAddress remote = new WorkerNetAddress().setHost("remote");
    WorkerNetAddress local = new WorkerNetAddress().setHost(WORKER_HOSTNAME_LOCAL);

    // Mock away Netty usage.
    ProtoMessage message = new ProtoMessage(
        Protocol.LocalBlockOpenResponse.newBuilder().setPath("/tmp").build());
    PowerMockito.mockStatic(NettyRPC.class);
    when(NettyRPC.call(Mockito.any(NettyRPCContext.class), Mockito.any(ProtoMessage.class)))
        .thenReturn(message);

    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(
        new BlockInfo().setLocations(Arrays.asList(new BlockLocation().setWorkerAddress(remote),
            new BlockLocation().setWorkerAddress(local))));
    assertEquals(local, mBlockStore
        .getInStream(BLOCK_ID, OpenUfsBlockOptions.getDefaultInstance(), InStreamOptions.defaults())
        .getAddress());
  }

  @Test
  public void getInStreamRemote() throws Exception {
    WorkerNetAddress remote1 = new WorkerNetAddress().setHost("remote1");
    WorkerNetAddress remote2 = new WorkerNetAddress().setHost("remote2");

    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(
        new BlockInfo().setLocations(Arrays.asList(new BlockLocation().setWorkerAddress(remote1),
            new BlockLocation().setWorkerAddress(remote2))));
    // We should sometimes get remote1 and sometimes get remote2.
    Set<WorkerNetAddress> results = new HashSet<>();
    for (int i = 0; i < 40; i++) {
      results.add(mBlockStore.getInStream(BLOCK_ID, OpenUfsBlockOptions.getDefaultInstance(),
          InStreamOptions.defaults()).getAddress());
    }
    assertEquals(Sets.newHashSet(remote1, remote2), results);
  }
}
