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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.util.network.NettyUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Closeable;
import java.util.Collections;

/**
 * Tests the {@link BlockInStream} class's static methods.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, NettyUtils.class})
public class BlockInStreamTest {
  private FileSystemContext mMockContext;
  private BlockInfo mInfo;
  private InStreamOptions mOptions;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private StreamObserver<OpenLocalBlockResponse> mResponseObserver;

  @Before
  public void before() throws Exception {
    BlockWorkerClient workerClient = PowerMockito.mock(BlockWorkerClient.class);
    ClientCallStreamObserver requestObserver = PowerMockito.mock(ClientCallStreamObserver.class);
    when(requestObserver.isReady()).thenReturn(true);
    when(workerClient.openLocalBlock(any(StreamObserver.class)))
        .thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            mResponseObserver = invocation.getArgumentAt(0, StreamObserver.class);
            return requestObserver;
          }
        });
    doAnswer(invocation -> {
      mResponseObserver.onNext(OpenLocalBlockResponse.newBuilder().setPath("/tmp").build());
      mResponseObserver.onCompleted();
      return null;
    }).when(requestObserver).onNext(any(OpenLocalBlockRequest.class));
    mMockContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.when(mMockContext.acquireBlockWorkerClient(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(workerClient);
    PowerMockito.when(mMockContext.getClientContext()).thenReturn(ClientContext.create(mConf));
    PowerMockito.when(mMockContext.getConf()).thenReturn(mConf);
    PowerMockito.doNothing().when(mMockContext)
        .releaseBlockWorkerClient(Matchers.any(WorkerNetAddress.class),
            Matchers.any(BlockWorkerClient.class));
    mInfo = new BlockInfo().setBlockId(1);
    mOptions = new InStreamOptions(new URIStatus(new FileInfo().setBlockIds(Collections
        .singletonList(1L))), mConf);
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
        new ConfigurationRule(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, "false", mConf)
            .toResource()) {
      WorkerNetAddress dataSource = new WorkerNetAddress();
      when(mMockContext.getClientContext()).thenReturn(ClientContext.create(mConf));
      BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
      BlockInStream stream =
          BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
      Assert.assertFalse(stream.isShortCircuit());
    }
  }

  @Test
  public void createDomainSocketEnabled() throws Exception {
    PowerMockito.mockStatic(NettyUtils.class);
    PowerMockito.when(NettyUtils.isDomainSocketSupported(Matchers.any(WorkerNetAddress.class),
        Matchers.any(InstancedConfiguration.class)))
        .thenReturn(true);
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.LOCAL;
    BlockInStream stream = BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType,
        mOptions);
    Assert.assertFalse(stream.isShortCircuit());
  }
}
