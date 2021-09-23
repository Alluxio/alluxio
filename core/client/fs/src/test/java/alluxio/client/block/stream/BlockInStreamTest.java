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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NettyUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
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
@PrepareForTest({NettyUtils.class})
public class BlockInStreamTest {
  private FileSystemContext mMockContext;
  private BlockInfo mInfo;
  private InStreamOptions mOptions;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private StreamObserver<OpenLocalBlockResponse> mResponseObserver;

  @Before
  public void before() throws Exception {
    BlockWorkerClient workerClient = Mockito.mock(BlockWorkerClient.class);
    ClientCallStreamObserver requestObserver = Mockito.mock(ClientCallStreamObserver.class);
    when(requestObserver.isReady()).thenReturn(true);
    when(workerClient.openLocalBlock(any(StreamObserver.class)))
        .thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            mResponseObserver = invocation.getArgument(0, StreamObserver.class);
            return requestObserver;
          }
        });
    doAnswer(invocation -> {
      mResponseObserver.onNext(OpenLocalBlockResponse.newBuilder().setPath("/tmp").build());
      mResponseObserver.onCompleted();
      return null;
    }).when(requestObserver).onNext(any(OpenLocalBlockRequest.class));
    mMockContext = Mockito.mock(FileSystemContext.class);
    when(mMockContext.acquireBlockWorkerClient(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(new NoopClosableResource<>(workerClient));
    when(mMockContext.getClientContext()).thenReturn(ClientContext.create(mConf));
    when(mMockContext.getClusterConf()).thenReturn(mConf);
    mInfo = new BlockInfo().setBlockId(1);
    mOptions = new InStreamOptions(new URIStatus(new FileInfo().setBlockIds(Collections
        .singletonList(1L))), mConf);
  }

  @Test
  public void closeReaderAfterReadingAllData() throws Exception {
    int chunkSize = 512;
    TestDataReader.Factory factory = new TestDataReader.Factory(
        chunkSize, BufferUtils.getIncreasingByteArray(2 * chunkSize));
    BlockInStream stream = new BlockInStream(factory, mConf, new WorkerNetAddress(),
        BlockInStream.BlockInStreamSource.PROCESS_LOCAL, -1, 1024);

    byte[] res = new byte[chunkSize];
    int read;
    read = stream.read(res, 0, chunkSize);
    TestDataReader reader = factory.getDataReader();
    assertEquals(chunkSize, read);
    assertNotNull(reader);
    assertFalse(reader.isClosed());

    // close data reader after reading all data
    read = stream.read(res, 0, chunkSize);
    assertEquals(chunkSize, read);
    assertTrue(reader.isClosed());

    read = stream.read(res, 0, chunkSize);
    assertEquals(-1, read);
    assertTrue(reader.isClosed());

    stream.close();
    assertTrue(reader.isClosed());
  }

  @Test
  public void createShortCircuit() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.NODE_LOCAL;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    assertEquals(LocalFileDataReader.Factory.class.getName(),
        stream.getDataReaderFactory().getClass().getName());
  }

  @Test
  public void createRemote() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.REMOTE;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    assertEquals(GrpcDataReader.Factory.class.getName(),
        stream.getDataReaderFactory().getClass().getName());
  }

  @Test
  public void createUfs() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.UFS;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    assertEquals(GrpcDataReader.Factory.class.getName(),
        stream.getDataReaderFactory().getClass().getName());
  }

  @Test
  public void createShortCircuitDisabled() throws Exception {
    try (Closeable c =
        new ConfigurationRule(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, "false", mConf)
            .toResource()) {
      WorkerNetAddress dataSource = new WorkerNetAddress();
      when(mMockContext.getClientContext()).thenReturn(ClientContext.create(mConf));
      BlockInStream.BlockInStreamSource dataSourceType =
          BlockInStream.BlockInStreamSource.NODE_LOCAL;
      BlockInStream stream =
          BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
      assertEquals(GrpcDataReader.Factory.class.getName(),
          stream.getDataReaderFactory().getClass().getName());
    }
  }

  @Test
  public void createDomainSocketEnabled() throws Exception {
    PowerMockito.mockStatic(NettyUtils.class);
    PowerMockito.when(NettyUtils.isDomainSocketAccessible(Matchers.any(WorkerNetAddress.class),
        Matchers.any(InstancedConfiguration.class)))
        .thenReturn(true);
    PowerMockito.when(NettyUtils.isDomainSocketSupported(Matchers.any(WorkerNetAddress.class)))
        .thenReturn(true);
    WorkerNetAddress dataSource = new WorkerNetAddress();
    BlockInStream.BlockInStreamSource dataSourceType = BlockInStream.BlockInStreamSource.NODE_LOCAL;
    BlockInStream stream = BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType,
        mOptions);
    assertEquals(GrpcDataReader.Factory.class.getName(),
        stream.getDataReaderFactory().getClass().getName());
  }

  @Test
  public void createProcessLocal() throws Exception {
    WorkerNetAddress dataSource = new WorkerNetAddress();
    when(mMockContext.getNodeLocalWorker()).thenReturn(dataSource);
    when(mMockContext.getClientContext()).thenReturn(ClientContext.create(mConf));
    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    when(mMockContext.getProcessLocalWorker()).thenReturn(blockWorker);
    BlockInStream.BlockInStreamSource dataSourceType =
        BlockInStream.BlockInStreamSource.PROCESS_LOCAL;
    BlockInStream stream =
        BlockInStream.create(mMockContext, mInfo, dataSource, dataSourceType, mOptions);
    assertEquals(BlockWorkerDataReader.Factory.class.getName(),
        stream.getDataReaderFactory().getClass().getName());
  }
}
