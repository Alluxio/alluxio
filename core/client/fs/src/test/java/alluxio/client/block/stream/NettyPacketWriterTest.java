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
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public final class NettyPacketWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(NettyPacketWriterTest.class);

  private static final int PACKET_SIZE = 1024;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4,
      ThreadFactoryUtils.build("test-executor-%d", true));

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;
  private static final int TIER = 0;

  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private EmbeddedChannels.EmbeddedEmptyCtorChannel mChannel;

  @Before
  public void before() throws Exception {
    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = Mockito.mock(WorkerNetAddress.class);

    mChannel = new EmbeddedChannels.EmbeddedEmptyCtorChannel();
    PowerMockito.when(mContext.acquireNettyChannel(mAddress)).thenReturn(mChannel);
    PowerMockito.doNothing().when(mContext).releaseNettyChannel(mAddress, mChannel);
  }

  @After
  public void after() throws Exception {
    mChannel.close();
  }

  /**
   * Writes an empty file.
   */
  @Test(timeout = 1000 * 60)
  public void writeEmptyFile() throws Exception {
    Future<Long> checksumActual;
    try (PacketWriter writer = create(10)) {
      checksumActual = verifyWriteRequests(mChannel, 0, 10);
    }
    Assert.assertEquals(0, checksumActual.get().longValue());
  }

  /**
   * Writes a file with file length matches what is given and verifies the checksum of the whole
   * file.
   */
  @Test(timeout = 1000 * 60)
  public void writeFullFile() throws Exception {
    Future<Long> checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(length)) {
      checksumExpected = writeFile(writer, length, 0, length - 1);
      checksumActual = verifyWriteRequests(mChannel, 0, length - 1);
      checksumExpected.get();
    }
    Assert.assertEquals(checksumExpected.get(), checksumActual.get());
  }

  /**
   * Writes a file with file length matches what is given and verifies the checksum of the whole
   * file.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileChecksumOfPartialFile() throws Exception {
    Future<Long> checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(length)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumActual = verifyWriteRequests(mChannel, 10, length / 3);
      checksumExpected.get();
    }
    Assert.assertEquals(checksumExpected.get(), checksumActual.get());
  }

  /**
   * Writes a file with unknown length.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileUnknownLength() throws Exception {
    Future<Long> checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 1024;
    try (PacketWriter writer = create(Long.MAX_VALUE)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumActual = verifyWriteRequests(mChannel, 10, length / 3);
      checksumExpected.get();
    }
    Assert.assertEquals(checksumExpected.get(), checksumActual.get());
  }

  /**
   * Writes lots of packets.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileManyPackets() throws Exception {
    Future<Long> checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 30000 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(Long.MAX_VALUE)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumActual = verifyWriteRequests(mChannel, 10, length / 3);
      checksumExpected.get();
    }
    Assert.assertEquals(checksumExpected.get(), checksumActual.get());
  }

  /**
   * Creates a {@link PacketWriter}.
   *
   * @param length the length
   * @return the packet writer instance
   */
  private PacketWriter create(long length) throws Exception {
    PacketWriter writer =
        new NettyPacketWriter(mContext, mAddress, BLOCK_ID, length, TIER,
            Protocol.RequestType.ALLUXIO_BLOCK, PACKET_SIZE);
    mChannel.finishChannelCreation();
    return writer;
  }

  /**
   * Writes packets via the given packet writer and returns a checksum for a region of the data
   * written.
   *
   * @param length the length
   * @param start the start position to calculate the checksum
   * @param end the end position to calculate the checksum
   * @return the checksum
   */
  private Future<Long> writeFile(final PacketWriter writer, final long length,
      final long start, final long end) throws Exception {
    return EXECUTOR.submit(new Callable<Long>() {
      @Override
      public Long call() throws IOException {
        try {
          long checksum = 0;
          long pos = 0;

          long remaining = length;
          while (remaining > 0) {
            int bytesToWrite = (int) Math.min(remaining, PACKET_SIZE);
            byte[] data = new byte[bytesToWrite];
            RANDOM.nextBytes(data);
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            try {
              writer.writePacket(buf);
            } catch (Exception e) {
              Assert.fail(e.getMessage());
              throw e;
            }
            remaining -= bytesToWrite;

            for (int i = 0; i < data.length; i++) {
              if (pos >= start && pos <= end) {
                checksum += BufferUtils.byteToInt(data[i]);
              }
              pos++;
            }
          }
          return checksum;
        } catch (Throwable throwable) {
          LOG.error("Failed to write file.", throwable);
          Assert.fail();
          throw throwable;
        }
      }
    });
  }

  /**
   * Verifies the packets written. After receiving the last packet, it will also send an EOF to
   * the channel.
   *
   * @param checksumStart the start position to calculate the checksum
   * @param checksumEnd the end position to calculate the checksum
   * @return the checksum of the data read starting from checksumStart
   */
  private Future<Long> verifyWriteRequests(final EmbeddedChannel channel, final long checksumStart,
      final long checksumEnd) {
    return EXECUTOR.submit(new Callable<Long>() {
      @Override
      public Long call() {
        try {
          long checksum = 0;
          long pos = 0;
          while (true) {
            RPCProtoMessage request = (RPCProtoMessage) CommonUtils
                .waitForResult("wrtie request", new Function<Void, Object>() {
                  @Override
                  public Object apply(Void v) {
                    return channel.readOutbound();
                  }
                }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
            validateWriteRequest(request.getMessage().asWriteRequest(), pos);

            DataBuffer buffer = request.getPayloadDataBuffer();
            // Last packet.
            if (buffer == null) {
              channel.writeInbound(RPCProtoMessage.createOkResponse(null));
              return checksum;
            }
            try {
              Assert.assertTrue(buffer instanceof DataNettyBufferV2);
              ByteBuf buf = (ByteBuf) buffer.getNettyOutput();
              while (buf.readableBytes() > 0) {
                if (pos >= checksumStart && pos <= checksumEnd) {
                  checksum += BufferUtils.byteToInt(buf.readByte());
                } else {
                  buf.readByte();
                }
                pos++;
              }
            } finally {
              buffer.release();
            }
          }
        } catch (Throwable throwable) {
          LOG.error("Failed to verify write requests.", throwable);
          Assert.fail();
          throw throwable;
        }
      }
    });
  }

  /**
   * Validates the read request sent.
   *
   * @param request the request
   * @param offset the offset
   */
  private void validateWriteRequest(Protocol.WriteRequest request, long offset) {
    Assert.assertEquals(BLOCK_ID, request.getId());
    Assert.assertEquals(offset, request.getOffset());
  }
}
