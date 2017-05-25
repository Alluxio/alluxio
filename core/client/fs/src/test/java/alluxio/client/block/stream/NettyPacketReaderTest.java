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

import alluxio.Constants;
import alluxio.EmbeddedChannels;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Function;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public final class NettyPacketReaderTest {
  private static final int PACKET_SIZE = 1024;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;

  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private EmbeddedChannels.EmbeddedEmptyCtorChannel mChannel;
  private NettyPacketReader.Factory mFactory;

  @Before
  public void before() throws Exception {
    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = Mockito.mock(WorkerNetAddress.class);
    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setBlockId(BLOCK_ID).setPacketSize(PACKET_SIZE).build();
    mFactory =
        new NettyPacketReader.Factory(mContext, mAddress, readRequest, InStreamOptions.defaults());

    mChannel = new EmbeddedChannels.EmbeddedEmptyCtorChannel();
    PowerMockito.when(mContext.acquireNettyChannel(mAddress)).thenReturn(mChannel);
    PowerMockito.doNothing().when(mContext).releaseNettyChannel(mAddress, mChannel);
  }

  @After
  public void after() throws Exception {
    mChannel.close();
  }

  /**
   * Reads an empty file.
   */
  @Test
  public void readEmptyFile() throws Exception {
    try (PacketReader reader = create(0, 10)) {
      sendReadResponses(mChannel, 0, 0, 0);
      Assert.assertEquals(null, reader.readPacket());
    }
    validateReadRequestSent(mChannel, 0, 10, false, PACKET_SIZE);
  }

  /**
   * Reads all contents in a file.
   */
  @Test(timeout = 1000 * 60)
  public void readFullFile() throws Exception {
    long length = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketReader reader = create(0, length)) {
      Future<Long> checksum = sendReadResponses(mChannel, length, 0, length - 1);

      long checksumActual = checkPackets(reader, 0, length);
      Assert.assertEquals(checksum.get().longValue(), checksumActual);
    }
    validateReadRequestSent(mChannel, 0, length, false, PACKET_SIZE);
  }

  /**
   * Reads part of a file and checks the checksum of the part that is read.
   */
  @Test(timeout = 1000 * 60)
  public void readPartialFile() throws Exception {
    long length = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    long offset = 10;
    long checksumStart = 100;
    long bytesToRead = length / 3;

    try (PacketReader reader = create(offset, length)) {
      Future<Long> checksum = sendReadResponses(mChannel, length, checksumStart, bytesToRead - 1);

      long checksumActual = checkPackets(reader, checksumStart, bytesToRead);
      Assert.assertEquals(checksum.get().longValue(), checksumActual);
    }
    validateReadRequestSent(mChannel, offset, length, false, PACKET_SIZE);
    validateReadRequestSent(mChannel, offset, length, true, PACKET_SIZE);
  }

  /**
   * Reads a file with unknown length.
   */
  @Test(timeout = 1000 * 60)
  public void fileLengthUnknown() throws Exception {
    long lengthActual = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    long checksumStart = 100;
    long bytesToRead = lengthActual / 3;

    try (PacketReader reader = create(0, Long.MAX_VALUE)) {
      Future<Long> checksum =
          sendReadResponses(mChannel, lengthActual, checksumStart, bytesToRead - 1);

      long checksumActual = checkPackets(reader, checksumStart, bytesToRead);
      Assert.assertEquals(checksum.get().longValue(), checksumActual);
    }
    validateReadRequestSent(mChannel, 0, Long.MAX_VALUE, false, PACKET_SIZE);
    validateReadRequestSent(mChannel, 0, Long.MAX_VALUE, true, PACKET_SIZE);
  }

  /**
   * Creates a {@link PacketReader}.
   *
   * @param offset the offset
   * @param length the length
   * @return the packet reader instance
   */
  private PacketReader create(long offset, long length) throws Exception {
    PacketReader reader = mFactory.create(offset, length);
    mChannel.finishChannelCreation();
    return reader;
  }

  /**
   * Reads the packets from the given {@link PacketReader}.
   *
   * @param reader the packet reader
   * @param checksumStart the start position to calculate the checksum
   * @param bytesToRead bytes to read
   * @return the checksum of the data read starting from checksumStart
   */
  private long checkPackets(PacketReader reader, long checksumStart, long bytesToRead)
      throws Exception {
    long pos = 0;
    long checksum = 0;

    while (true) {
      DataBuffer packet = reader.readPacket();
      if (packet == null) {
        break;
      }
      try {
        Assert.assertTrue(packet instanceof DataNettyBufferV2);
        ByteBuf buf = (ByteBuf) packet.getNettyOutput();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
          if (pos >= checksumStart) {
            checksum += BufferUtils.byteToInt(bytes[i]);
          }
          pos++;
          if (pos >= bytesToRead) {
            return checksum;
          }
        }
      } finally {
        packet.release();
      }
    }
    return checksum;
  }

  /**
   * Validates the read request sent.
   *
   * @param channel the channel
   * @param offset the offset
   * @param length the length
   * @param cancel whether it is a cancel request
   * @param packetSize the packet size
   */
  private void validateReadRequestSent(final EmbeddedChannel channel, long offset, long length,
      boolean cancel, long packetSize) {
    Object request = CommonUtils.waitForResult("read request", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readOutbound();
      }
    }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));

    Assert.assertTrue(request != null);
    Assert.assertTrue(request instanceof RPCProtoMessage);
    Assert.assertEquals(null, ((RPCProtoMessage) request).getPayloadDataBuffer());
    Protocol.ReadRequest readRequest = ((RPCProtoMessage) request).getMessage().asReadRequest();
    Assert.assertEquals(BLOCK_ID, readRequest.getBlockId());
    Assert.assertEquals(offset, readRequest.getOffset());
    Assert.assertEquals(length, readRequest.getLength());
    Assert.assertEquals(cancel, readRequest.getCancel());
    Assert.assertEquals(packetSize, readRequest.getPacketSize());
  }

  /**
   * Sends read responses to the channel.
   *
   * @param channel the channel
   * @param length the length
   * @param start the start position to calculate the checksum
   * @param end the end position to calculate the checksum
   * @return the checksum
   */
  private Future<Long> sendReadResponses(final EmbeddedChannel channel, final long length,
      final long start, final long end) {
    ProtoMessage heartbeat = new ProtoMessage(
        Protocol.ReadResponse.newBuilder().setType(Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT)
            .build());
    // Send some heartbeats first.
    for (int i = 0; i < 3; ++i) {
      channel.writeInbound(new RPCProtoMessage(heartbeat));
    }
    return EXECUTOR.submit(new Callable<Long>() {
      @Override
      public Long call() {
        long checksum = 0;
        long pos = 0;

        long remaining = length;
        while (remaining > 0) {
          int bytesToSend = (int) Math.min(remaining, PACKET_SIZE);
          byte[] data = new byte[bytesToSend];
          RANDOM.nextBytes(data);
          ByteBuf buf = Unpooled.wrappedBuffer(data);
          RPCProtoMessage message = RPCProtoMessage.createOkResponse(new DataNettyBufferV2(buf));
          channel.writeInbound(message);
          remaining -= bytesToSend;

          for (int i = 0; i < data.length; i++) {
            if (pos >= start && pos <= end) {
              checksum += BufferUtils.byteToInt(data[i]);
            }
            pos++;
          }
        }

        // send EOF.
        channel.writeInbound(RPCProtoMessage.createOkResponse(null));
        return checksum;
      }
    });
  }
}
