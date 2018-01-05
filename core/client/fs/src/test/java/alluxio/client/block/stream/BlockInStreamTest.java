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

package alluxio.client.block.stream;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Closeable;
import java.util.Collections;

/**
 * Tests the {@link BlockInStream} class's static methods.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, NettyRPC.class, NettyUtils.class})
public class BlockInStreamTest {
  private FileSystemContext mMockContext;
  private BlockInfo mInfo;
  private InStreamOptions mOptions;

  @Before
  public void before() throws Exception {
    Channel mockChannel = PowerMockito.mock(Channel.class);
    PowerMockito.mockStatic(NettyRPC.class);
    PowerMockito.when(
        NettyRPC.call(Matchers.any(NettyRPCContext.class), Matchers.any(ProtoMessage.class)))
        .thenReturn(new ProtoMessage(Protocol.LocalBlockOpenResponse.getDefaultInstance()));
    mMockContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.when(mMockContext.acquireNettyChannel(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(mockChannel);
    PowerMockito.doNothing().when(mMockContext)
        .releaseNettyChannel(Matchers.any(WorkerNetAddress.class), Matchers.any(Channel.class));
    mInfo = new BlockInfo().setBlockId(1);
    mOptions = new InStreamOptions(new URIStatus(new FileInfo().setBlockIds(Collections
        .singletonList(1L))));
  }

  @Test
  public void createShortCircuit() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    Assert.assertTrue(stream.isShortCircuit());
  }

  @Test
  public void createRemote() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.REMOTE;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    Assert.assertFalse(stream.isShortCircuit());
  }

  @Test
  public void createUfs() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.UFS;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    Assert.assertFalse(stream.isShortCircuit());
  }

  @Test
  public void createShortCircuitDisabled() throws Exception {
    try (Closeable c =
        new ConfigurationRule(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, "false").toResource()) {
      WorkerNetAddress dataSource = new WorkerNetAddress();
      BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
      BlockInStream stream =
          BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
      Assert.assertFalse(stream.isShortCircuit());
    }
  }

  @Test
  public void createDomainSocketEnabled() throws Exception {
    PowerMockito.mockStatic(NettyUtils.class);
    PowerMockito.when(NettyUtils.isDomainSocketSupported(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(true);
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    Assert.assertFalse(stream.isShortCircuit());
  }
}
