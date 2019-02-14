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

import alluxio.grpc.RequestType;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for {@link BlockWriteHandler}.
 */
public final class BlockWriteHandlerTest extends AbstractWriteHandlerTest {
  private BlockWorker mBlockWorker;
  private BlockWriter mBlockWriter;
  private File mFile;

  @Before
  public void before() throws Exception {
    mFile = mTestFolder.newFile();
    mBlockWorker = Mockito.mock(BlockWorker.class);
    Mockito.doNothing().when(mBlockWorker)
        .createBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(),
            Mockito.anyLong());
    Mockito.doNothing().when(mBlockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    Mockito.doNothing().when(mBlockWorker).abortBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doNothing().when(mBlockWorker).commitBlock(Mockito.anyLong(), Mockito.anyLong());
    mBlockWriter = new LocalFileBlockWriter(mFile.getPath());
    Mockito.when(mBlockWorker.getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mBlockWriter)
        .thenReturn(new LocalFileBlockWriter(mTestFolder.newFile().getPath()));
    mResponseObserver = Mockito.mock(StreamObserver.class);
    mWriteHandler = new BlockWriteHandler(mBlockWorker, mResponseObserver);
  }

  @Test
  public void writeFailure() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mBlockWriter.close();
    mWriteHandler.write(newWriteRequest(newDataBuffer(CHUNK_SIZE)));
    checkErrorCode(mResponseObserver, Status.Code.FAILED_PRECONDITION);
  }

  @Override
  protected RequestType getWriteRequestType() {
    return RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected InputStream getWriteDataStream() throws IOException {
    return new FileInputStream(mFile);
  }
}
