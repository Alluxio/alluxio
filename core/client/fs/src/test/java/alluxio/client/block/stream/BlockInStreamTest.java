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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
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

/**
 * Tests the {@link BlockInStream} class's static methods.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, NettyRPC.class, NettyUtils.class})
public class BlockInStreamTest {
  private FileSystemContext mMockContext;

  @Before
  public void before() throws Exception {
    PowerMockito.mockStatic(NettyRPC.class);
    PowerMockito.when(
        NettyRPC.call(Matchers.any(NettyRPCContext.class), Matchers.any(ProtoMessage.class)))
        .thenReturn(new ProtoMessage(Protocol.LocalBlockOpenResponse.getDefaultInstance()));
    mMockContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.when(mMockContext.acquireNettyChannel(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(null);
    PowerMockito.doNothing().when(mMockContext)
        .releaseNettyChannel(Matchers.any(WorkerNetAddress.class), Matchers.any(Channel.class));
  }

  @Test
  public void createShortCircuit() throws Exception {
    BlockInfo info = new BlockInfo();
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
    InStreamOptions options = new InStreamOptions(new URIStatus(new FileInfo()));
    BlockInStream stream = BlockInStream.create(mMockContext, info, dataSource, dataSourceType, options);
    Assert.assertTrue(stream.isShortCircuit());
  }

  @Test
  public void createRemote() throws Exception {
    BlockInfo info = new BlockInfo();
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.REMOTE;
    InStreamOptions options = new InStreamOptions(new URIStatus(new FileInfo()));
    BlockInStream stream = BlockInStream.create(mMockContext, info, dataSource, dataSourceType, options);
    Assert.assertFalse(stream.isShortCircuit());
  }

  @Test
  public void createUfs() throws Exception {
    BlockInfo info = new BlockInfo();
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.UFS;
    InStreamOptions options = new InStreamOptions(new URIStatus(new FileInfo()));
    BlockInStream stream = BlockInStream.create(mMockContext, info, dataSource, dataSourceType, options);
    Assert.assertFalse(stream.isShortCircuit());
  }

  @Test
  public void createShortCircuitDisabled() throws Exception {
    Configuration.set(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false);
    try {
      BlockInfo info = new BlockInfo();
      WorkerNetAddress dataSource = new WorkerNetAddress();
      BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
      InStreamOptions options = new InStreamOptions(new URIStatus(new FileInfo()));
      BlockInStream stream = BlockInStream.create(mMockContext, info, dataSource, dataSourceType, options);
      Assert.assertFalse(stream.isShortCircuit());
    } finally {
      ConfigurationTestUtils.resetConfiguration();
    }
  }

  @Test
  public void createDomainSocketEnabled() throws Exception {
    PowerMockito.mockStatic(NettyUtils.class);
    PowerMockito.when(NettyUtils.isDomainSocketSupported(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(true);
    BlockInfo info = new BlockInfo();
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
    InStreamOptions options = new InStreamOptions(new URIStatus(new FileInfo()));
    BlockInStream stream =
        BlockInStream.create(mMockContext, info, dataSource, dataSourceType, options);
    Assert.assertFalse(stream.isShortCircuit());
  }
}
