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

package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.netty.channel.embedded.EmbeddedChannel;
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
    mChannel = new EmbeddedChannel(
        new BlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
  }

  @Test
  public void writeFailure() throws Exception {
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    mBlockWriter.close();
    mChannel.writeInbound(newWriteRequest(PACKET_SIZE, newDataBuffer(PACKET_SIZE)));
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.FAILED_PRECONDITION, writeResponse);
  }

  @Override
  protected Protocol.RequestType getWriteRequestType() {
    return Protocol.RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected InputStream getWriteDataStream() throws IOException {
    return new FileInputStream(mFile);
  }
}
