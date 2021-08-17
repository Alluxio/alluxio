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

import static org.mockito.Mockito.mock;

import alluxio.AlluxioTestDirectory;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Tests {@link LocalFileDataWriterTest}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class, LocalFileDataWriter.class,
    GrpcBlockingStream.class})
public class LocalFileDataWriterTest {
  private static final long BLOCK_ID = 1L;

  protected String mWorkDirectory;

  private WorkerNetAddress mAddress;
  private BlockWorkerClient mClient;
  private ClientContext mClientContext;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemContext mContext;
  private GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> mStream;

  @Before
  public void before() throws Exception {
    mWorkDirectory =
        AlluxioTestDirectory.createTemporaryDirectory("blocks").getAbsolutePath();

    mClientContext = ClientContext.create(mConf);

    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = mock(WorkerNetAddress.class);

    mClient = mock(BlockWorkerClient.class);
    PowerMockito.when(mContext.acquireBlockWorkerClient(mAddress)).thenReturn(
        new NoopClosableResource<>(mClient));
    PowerMockito.when(mContext.getClientContext()).thenReturn(mClientContext);
    PowerMockito.when(mContext.getClusterConf()).thenReturn(mConf);

    mStream = mock(GrpcBlockingStream.class);
    PowerMockito.doNothing().when(mStream).send(Matchers.any(), Matchers.anyLong());
    PowerMockito.when(mStream.receive(Matchers.anyLong()))
        .thenReturn(CreateLocalBlockResponse.newBuilder()
            .setPath(PathUtils.temporaryFileName(IdUtils.getRandomNonNegativeLong(),
                PathUtils.concatPath(mWorkDirectory, BLOCK_ID)))
            .build());
    PowerMockito.when(mStream.isCanceled()).thenReturn(false);
    PowerMockito.when(mStream.isClosed()).thenReturn(false);
    PowerMockito.when(mStream.isOpen()).thenReturn(true);

    PowerMockito.whenNew(GrpcBlockingStream.class).withAnyArguments().thenReturn(mStream);
  }

  @After
  public void after() throws Exception {
    mClient.close();
  }

  @Test
  public void streamCancelled() throws Exception {
    LocalFileDataWriter writer = LocalFileDataWriter.create(mContext, mAddress, BLOCK_ID,
        128 /* unused */, OutStreamOptions.defaults(mClientContext));

    // Cancel stream before cancelling the writer
    PowerMockito.when(mStream.isCanceled()).thenReturn(true);
    PowerMockito.when(mStream.isClosed()).thenReturn(true);
    PowerMockito.when(mStream.isOpen()).thenReturn(true);

    writer.cancel();

    // Verify there are no open files
    LocalFileBlockWriter blockWriter = Whitebox.getInternalState(writer, "mWriter");
    Assert.assertTrue(Whitebox.getInternalState(blockWriter, "mClosed"));
  }

  @Test
  public void streamClosed() throws Exception {
    LocalFileDataWriter writer = LocalFileDataWriter.create(mContext, mAddress, BLOCK_ID,
        128 /* unused */, OutStreamOptions.defaults(mClientContext));

    // Close stream before closing the writer
    PowerMockito.when(mStream.isCanceled()).thenReturn(true);
    PowerMockito.when(mStream.isClosed()).thenReturn(true);
    PowerMockito.when(mStream.isOpen()).thenReturn(true);

    writer.close();

    // Verify there are no open files
    LocalFileBlockWriter blockWriter = Whitebox.getInternalState(writer, "mWriter");
    Assert.assertTrue(Whitebox.getInternalState(blockWriter, "mClosed"));
  }
}
