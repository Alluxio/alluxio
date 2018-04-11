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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.EmbeddedNoExceptionChannel;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
    mChannel = new EmbeddedChannel(
        new BlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
    mChannelNoException = new EmbeddedNoExceptionChannel(
        new BlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
  }

  /**
   * Tests the {@link FileTransferType#TRANSFER} type.
   */
  @Test
  public void transferType() throws Exception {
    mChannel = new EmbeddedChannel(
        new BlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.TRANSFER));

    long fileSize = PACKET_SIZE * 2;
    long checksumExpected = populateInputFile(fileSize, 0, fileSize - 1);

    BlockReader blockReader = spy(mBlockReader);
    // Do not call close here so that we can check result. It will be closed explicitly.
    doNothing().when(blockReader).close();
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenReturn(blockReader);
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    checkAllReadResponses(mChannel, checksumExpected);
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
    mChannelNoException.writeInbound(buildReadRequest(0, fileSize));
    Object response = waitForOneResponse(mChannelNoException);
    checkReadResponse(response, PStatus.FAILED_PRECONDITION);
  }

  @Override
  protected void mockReader(long start) throws Exception {
    mBlockReader = new LocalFileBlockReader(mFile);
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenReturn(mBlockReader);
  }

  @Override
  protected RPCProtoMessage buildReadRequest(long offset, long len) {
    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setBlockId(1L).setOffset(offset).setLength(len)
            .setPacketSize(PACKET_SIZE).build();
    return new RPCProtoMessage(new ProtoMessage(readRequest), null);
  }
}
