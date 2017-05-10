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

import alluxio.EmbeddedChannelNoException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class, LocalFileBlockReader.class})
public final class DataServerBlockReadHandlerTest extends DataServerReadHandlerTest {
  private BlockWorker mBlockWorker;
  private BlockReader mBlockReader;

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    mBlockWorker = PowerMockito.mock(BlockWorker.class);
    PowerMockito.doNothing().when(mBlockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
    mChannel = new EmbeddedChannel(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
    mChannelNoException = new EmbeddedChannelNoException(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
  }

  @Test
  public void transferType() throws Exception {
    mChannel = new EmbeddedChannel(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.TRANSFER));

    long fileSize = PACKET_SIZE * 2;
    long checksumExpected = populateInputFile(fileSize, 0, fileSize - 1);

    BlockReader blockReader = PowerMockito.spy(mBlockReader);
    // Do not call close here so that we can check result. It will be closed explicitly.
    PowerMockito.doNothing().when(blockReader).close();
    PowerMockito
        .when(mBlockWorker.readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(blockReader);
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    checkAllReadResponses(mChannel, checksumExpected);
    mBlockReader.close();
  }

  @Test
  public void readFailure() throws Exception {
    long fileSize = PACKET_SIZE * 10 + 1;
    populateInputFile(0, 0, fileSize - 1);
    mBlockReader.close();
    mChannelNoException.writeInbound(buildReadRequest(0, fileSize));
    Object response = waitForOneResponse(mChannelNoException);
    checkReadResponse(response, Protocol.Status.Code.INTERNAL);
  }

  @Override
  protected void mockReader(long start) throws Exception {
    mBlockReader = new LocalFileBlockReader(mFile);
    PowerMockito
        .when(mBlockWorker.readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mBlockReader);
  }

  @Override
  protected RPCProtoMessage buildReadRequest(long offset, long len) {
    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setId(1L).setOffset(offset).setLength(len)
            .setLockId(1L).setType(Protocol.RequestType.ALLUXIO_BLOCK).build();
    return new RPCProtoMessage(readRequest, null);
  }
}
