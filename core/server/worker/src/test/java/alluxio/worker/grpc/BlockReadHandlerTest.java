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

package alluxio.worker.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.proto.status.Status;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalFileBlockReader.class})
public final class BlockReadHandlerTest extends ReadHandlerTest {
  private BlockWorker mBlockWorker;
  private BlockReader mBlockReader;

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    mBlockWorker = mock(BlockWorker.class);
    doNothing().when(mBlockWorker).accessBlock(anyLong(), anyLong());
    mResponseObserver = Mockito.mock(ServerCallStreamObserver.class);
    Mockito.when(mResponseObserver.isReady()).thenReturn(true);
    mReadHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED);
    mReadHandlerNoException = new BlockReadHandler(
        GrpcExecutors.BLOCK_READER_EXECUTOR, mBlockWorker, FileTransferType.MAPPED);
  }

  /**
   * Tests the {@link FileTransferType#TRANSFER} type.
   */
  @Test
  public void transferType() throws Exception {
    BlockReadHandler readHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR,
        mBlockWorker, FileTransferType.TRANSFER);

    long fileSize = PACKET_SIZE * 2;
    long checksumExpected = populateInputFile(fileSize, 0, fileSize - 1);

    BlockReader blockReader = spy(mBlockReader);
    // Do not call close here so that we can check result. It will be closed explicitly.
    doNothing().when(blockReader).close();
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenReturn(blockReader);
    readHandler.readBlock(buildReadRequest(0, fileSize), mResponseObserver);
    checkAllReadResponses(mResponseObserver, checksumExpected);
    mBlockReader.close();
  }

  /**
   * Tests read failure.
   */
  @Test
  public void readFailure() throws Exception {
    long fileSize = PACKET_SIZE * 10 + 1;
    populateInputFile(0, 0, fileSize - 1);
    mBlockReader.close();
    mReadHandlerNoException.readBlock(buildReadRequest(0, fileSize), mResponseObserver);
    ReadResponse response = waitForOneResponse(mResponseObserver);
    checkReadResponse(response, Status.PStatus.FAILED_PRECONDITION);
  }

  @Override
  protected void mockReader(long start) throws Exception {
    mBlockReader = new LocalFileBlockReader(mFile);
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenReturn(mBlockReader);
  }

  @Override
  protected ReadRequest buildReadRequest(long offset, long len) {
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(1L).setOffset(offset).setLength(len)
            .build();
    return readRequest;
  }
}
