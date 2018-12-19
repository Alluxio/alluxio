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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.when;

import alluxio.exception.status.FailedPreconditionException;
import alluxio.grpc.ReadRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.grpc.stub.ServerCallStreamObserver;
import io.netty.util.ResourceLeakDetector;
import org.junit.Assert;
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
    doAnswer(args -> {
      mResponseCompleted = true;
      return null;
    }).when(mResponseObserver).onCompleted();
    doAnswer(args -> {
      mError = args.getArgumentAt(0, Throwable.class);
      return null;
    }).when(mResponseObserver).onError(any(Throwable.class));
    mReadHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR, mBlockWorker);
    mReadHandlerNoException = new BlockReadHandler(
        GrpcExecutors.BLOCK_READER_EXECUTOR, mBlockWorker);
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
    Throwable t = waitForError(mResponseObserver);
    Assert.assertTrue(t instanceof FailedPreconditionException);
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
