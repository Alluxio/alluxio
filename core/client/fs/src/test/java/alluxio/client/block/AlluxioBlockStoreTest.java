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

import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.netty.NettyRPC;
import alluxio.client.netty.NettyRPCContext;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.DummyCloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
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
import java.io.OutputStream;
import java.util.List;

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
      if (mWorkerNetAddresses.isEmpty()) {
        return null;
      }
      return mWorkerNetAddresses.get(mIndex++);
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
    Mockito.when(mContext.acquireBlockMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(mMasterClient));
    mLocalAddr = new WorkerNetAddress().setHost(NetworkAddressUtils.getLocalHostName());

    mBlockStore = new AlluxioBlockStore(mContext, WORKER_HOSTNAME_LOCAL);

    Mockito.when(mContext.acquireNettyChannel(Mockito.any(WorkerNetAddress.class)))
        .thenReturn(mChannel);
    Mockito.when(mChannel.pipeline()).thenReturn(mPipeline);
    Mockito.when(mPipeline.last()).thenReturn(new RPCMessageDecoder());
    Mockito.when(mPipeline.addLast(Mockito.any(ChannelHandler.class))).thenReturn(mPipeline);
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
    Mockito.when(NettyRPC.call(Mockito.any(NettyRPCContext.class), Mockito.any(ProtoMessage.class)))
        .thenReturn(message);

    OutStreamOptions options = OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_LOCAL)))
        .setWriteType(WriteType.MUST_CACHE);
    OutputStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(alluxio.client.block.stream.BlockOutStream.class, stream.getClass());
  }

  @Test
  public void getOutStreamRemote() throws Exception {
    OutStreamOptions options = OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_REMOTE)))
        .setWriteType(WriteType.MUST_CACHE);
    OutputStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(alluxio.client.block.stream.BlockOutStream.class, stream.getClass());
  }
}
