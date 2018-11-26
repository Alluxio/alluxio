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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public class UfsFallbackLocalFilePacketWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(NettyPacketWriterTest.class);

  /**
   * A packet writer implementation which will throw a ResourceExhaustedException on writes when the
   * given ByteBuffer is full.
   */
  public static class FixedCapacityTestPacketWriter extends TestPacketWriter {
    private final long mCapacity;
    private final ByteBuffer mBuffer;
    private boolean mIsLocalWorkerFull = false;
    private boolean mClosed = false;
    private boolean mCanceled = false;

    public FixedCapacityTestPacketWriter(ByteBuffer buffer) {
      super(buffer);
      mCapacity = buffer.capacity();
      mBuffer = buffer;
    }

    @Override
    public void writePacket(ByteBuf packet) throws IOException {
      if (pos() + packet.readableBytes() > mCapacity) {
        mIsLocalWorkerFull = true;
      }
      if (mIsLocalWorkerFull) {
        throw new ResourceExhaustedException("no more space!");
      }
      synchronized (mBuffer) {
        super.writePacket(packet);
      }
    }

    @Override
    public void close() {
      super.close();
      if (mClosed) {
        return;
      }
      mClosed = true;
    }

    @Override
    public void cancel() {
      super.cancel();
      if (mCanceled) {
        return;
      }
      mCanceled = true;
      mClosed = true;
    }

    public boolean isClosed() {
      return mClosed;
    }

    public boolean isCanceled() {
      return mCanceled;
    }
  }

  private static final int PACKET_SIZE = 1024;
  private static final ExecutorService EXECUTOR =
      Executors.newFixedThreadPool(4, ThreadFactoryUtils.build("test-executor-%d", true));

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;
  private static final long MOUNT_ID = 9L;

  private ByteBuffer mBuffer;
  private FixedCapacityTestPacketWriter mLocalWriter;
  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private EmbeddedChannel mChannel;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES,
          String.valueOf(PACKET_SIZE));

  @Before
  public void before() throws Exception {
    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = Mockito.mock(WorkerNetAddress.class);

    mChannel = new EmbeddedChannel();
    PowerMockito.when(mContext.acquireNettyChannel(mAddress)).thenReturn(mChannel);
    PowerMockito.doNothing().when(mContext).releaseNettyChannel(mAddress, mChannel);
  }

  @After
  public void after() throws Exception {
    mChannel.close();
  }

  /**
   * Creates a {@link PacketWriter}.
   *
   * @param blockSize the block length
   * @param workerCapacity the capacity of the local worker
   * @return the packet writer instance
   */
  private PacketWriter create(long blockSize, long workerCapacity) throws Exception {
    mBuffer = ByteBuffer.allocate((int) workerCapacity);
    mLocalWriter = new FixedCapacityTestPacketWriter(mBuffer);
    PacketWriter writer =
        new UfsFallbackLocalFilePacketWriter(mLocalWriter, null, mContext, mAddress, BLOCK_ID,
            blockSize, OutStreamOptions.defaults().setMountId(MOUNT_ID));
    return writer;
  }

  @Test
  public void emptyBlock() throws Exception {
    try (PacketWriter writer = create(1, 1)) {
      writer.flush();
      assertEquals(0, writer.pos());
    }
    assertEquals(0, mBuffer.position());
  }

  @Test(timeout = 1000 * 60)
  public void noFallback() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    long blockSize = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(blockSize, blockSize)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      expected.get();
    }
    assertEquals(expected.get().getBytes(), actualLocal.get().getBytes());
    assertEquals(expected.get().getChecksum(), actualLocal.get().getChecksum());
  }

  @Ignore("Flaky test")
  @Test(timeout = 1000 * 60)
  public void fallbackOnFirstPacket() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    Future<WriteSummary> actualUfs;
    long blockSize = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(blockSize, 1)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      actualUfs = getUfsWrite(mChannel);
      expected.get();
    }
    assertEquals(blockSize, expected.get().getBytes());
    assertEquals(0, actualLocal.get().getBytes());
    assertEquals(blockSize, actualUfs.get().getBytes());
    assertEquals(expected.get().getBytes(), actualUfs.get().getBytes());
    assertEquals(expected.get().getChecksum(), actualUfs.get().getChecksum());
  }

  @Ignore("Flaky test")
  @Test(timeout = 1000 * 60)
  public void fallbackOnSecondPacket() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    Future<WriteSummary> actualUfs;
    long blockSize = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(blockSize, PACKET_SIZE)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      actualUfs = getUfsWrite(mChannel);
      expected.get();
    }
    assertEquals(blockSize, expected.get().getBytes());
    assertEquals(PACKET_SIZE, actualLocal.get().getBytes());
    assertEquals(blockSize - PACKET_SIZE, actualUfs.get().getBytes());
    assertEquals(expected.get().getChecksum(),
        actualLocal.get().getChecksum() + actualUfs.get().getChecksum());
  }

  @Test(timeout = 1000 * 60)
  public void fallbackOnLastPacket() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    Future<WriteSummary> actualUfs;
    long blockSize = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(blockSize, PACKET_SIZE * 1024)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      actualUfs = getUfsWrite(mChannel);
      expected.get();
    }
    assertEquals(blockSize, expected.get().getBytes());
    assertEquals(PACKET_SIZE * 1024, actualLocal.get().getBytes());
    assertEquals(blockSize - PACKET_SIZE * 1024, actualUfs.get().getBytes());
    assertEquals(expected.get().getChecksum(),
        actualLocal.get().getChecksum() + actualUfs.get().getChecksum());
  }

  @Test(timeout = 1000 * 60)
  public void flush() throws Exception {
    long blockSize = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(blockSize, PACKET_SIZE)) {
      Future<WriteSummary> expected;
      expected = writeData(writer, PACKET_SIZE);
      expected.get();
      writer.flush();
      assertEquals(PACKET_SIZE, mBuffer.position());
    }
  }

  @Test(timeout = 1000 * 60)
  public void pos() throws Exception {
    long blockSize = PACKET_SIZE * 2 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(blockSize, PACKET_SIZE)) {
      byte[] data = new byte[1];
      Future<WriteSummary> actualUfs = getUfsWrite(mChannel);
      for (long pos = 0; pos < blockSize; pos++) {
        assertEquals(pos, writer.pos());
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        writer.writePacket(buf);
      }
      actualUfs.get();
    }
  }

  class WriteSummary {
    private final long mBytes;
    private final long mChecksum;

    public WriteSummary(long bytes, long checksum) {
      mBytes = bytes;
      mChecksum = checksum;
    }

    public long getBytes() {
      return mBytes;
    }

    public long getChecksum() {
      return mChecksum;
    }
  }

  /**
   * Writes packets via the given packet writer and returns a checksum for a region of the data
   * written.
   *
   * @param length the length
   * @return the checksum
   */
  private Future<WriteSummary> writeData(final PacketWriter writer, final long length)
      throws Exception {
    return EXECUTOR.submit(new Callable<WriteSummary>() {
      @Override
      public WriteSummary call() throws IOException {
        try {
          long checksum = 0;
          long remaining = length;
          while (remaining > 0) {
            int bytesToWrite = (int) Math.min(remaining, PACKET_SIZE);
            byte[] data = new byte[bytesToWrite];
            RANDOM.nextBytes(data);
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            try {
              writer.writePacket(buf);
            } catch (Exception e) {
              fail(e.getMessage());
              throw e;
            }
            remaining -= bytesToWrite;
            // TODO(binfan): create a util method to calculate checksum from buffer
            for (int i = 0; i < data.length; i++) {
              checksum += BufferUtils.byteToInt(data[i]);
            }
          }
          return new WriteSummary(length, checksum);
        } catch (Throwable throwable) {
          LOG.error("Failed to write file.", throwable);
          fail("Failed to write file." + throwable.getMessage());
          throw throwable;
        }
      }
    });
  }

  /**
   * Verifies the packets written. After receiving the last packet, it will also send an EOF to
   * the channel.
   *
   * @return the checksum of the data read starting from checksumStart
   */
  private Future<WriteSummary> getUfsWrite(final EmbeddedChannel channel) {
    return EXECUTOR.submit(new Callable<WriteSummary>() {
      @Override
      public WriteSummary call() throws TimeoutException, InterruptedException {
        try {
          long checksum = 0;
          long pos = 0;
          long len = 0;
          while (true) {
            RPCProtoMessage request = (RPCProtoMessage) CommonUtils.waitForResult("write request",
                () -> channel.readOutbound(),
                WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
            Protocol.WriteRequest writeRequest = request.getMessage().asWriteRequest();
            validateWriteRequest(writeRequest, pos);
            DataBuffer buffer = request.getPayloadDataBuffer();
            // Last packet.
            if (writeRequest.hasEof() && writeRequest.getEof()) {
              assertTrue(buffer == null);
              channel.writeInbound(RPCProtoMessage.createOkResponse(null));
              return new WriteSummary(len, checksum);
            }
            // UFS block init
            if (writeRequest.getCreateUfsBlockOptions().hasBytesInBlockStore()) {
              assertTrue(buffer == null);
              pos += writeRequest.getCreateUfsBlockOptions().getBytesInBlockStore();
              continue;
            }
            try {
              Assert.assertTrue(buffer instanceof DataNettyBufferV2);
              ByteBuf buf = (ByteBuf) buffer.getNettyOutput();
              while (buf.readableBytes() > 0) {
                checksum += BufferUtils.byteToInt(buf.readByte());
                pos++;
                len++;
              }
            } finally {
              buffer.release();
            }
          }
        } catch (Throwable throwable) {
          fail("Failed to verify write requests." + throwable.getMessage());
          throw throwable;
        }
      }
    });
  }

  private Future<WriteSummary> getLocalWrite(final ByteBuffer buffer) {
    return EXECUTOR.submit(new Callable<WriteSummary>() {
      @Override
      public WriteSummary call() throws TimeoutException, InterruptedException {
        long checksum = 0;
        long pos = 0;
        CommonUtils.waitFor("Writing to local completes", () -> mLocalWriter.isClosed());
        synchronized (buffer) {
          int len = buffer.position();
          while (pos < len) {
            checksum += BufferUtils.byteToInt(buffer.get((int) pos));
            pos++;
          }
        }
        return new WriteSummary(pos, checksum);
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
    assertEquals(Protocol.RequestType.UFS_FALLBACK_BLOCK, request.getType());
    assertEquals(BLOCK_ID, request.getId());
    assertEquals(offset, request.getOffset());
    assertTrue(request.hasCreateUfsBlockOptions());
    assertEquals(MOUNT_ID, request.getCreateUfsBlockOptions().getMountId());
  }
}
