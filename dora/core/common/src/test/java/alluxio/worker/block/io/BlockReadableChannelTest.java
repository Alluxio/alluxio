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

package alluxio.worker.block.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public class BlockReadableChannelTest {
  private static final int DATA_SIZE = Constants.KB;
  private BlockReadableChannel mChannel;
  private final BlockReader mReader =
      new MockBlockReader(BufferUtils.getIncreasingByteArray(DATA_SIZE));

  @Before
  public void setup() throws Exception {
    mChannel = new BlockReadableChannel(mReader);
  }

  @Test
  public void closed() throws Exception {
    assertTrue(mChannel.isOpen());
    mChannel.close();
    assertThrows(ClosedChannelException.class, () -> mChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void read() throws Exception {
    int headerSize = 1;
    int trailerSize = 1;
    ByteBuffer buffer = ByteBuffer.allocate(DATA_SIZE + headerSize + trailerSize);
    buffer.clear();
    buffer.put((byte) 0x42); // fill with some data so that the position starts with non-zero
    assertEquals(DATA_SIZE, IOUtils.read(mChannel, buffer));
    assertEquals(headerSize + DATA_SIZE, buffer.position());
    buffer.flip();
    assertEquals((byte) 0x42, buffer.get());
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, DATA_SIZE, buffer.slice()));
  }

  /**
   * Calling {@link BlockReadableChannel#read(ByteBuffer)} and
   * {@link BlockReader#transferTo(ByteBuf)} shares the same internal position.
   */
  @Test
  public void dependentInternalPosition() throws Exception {
    int dataSize = 100;
    ByteBuffer buffer = ByteBuffer.allocate(dataSize);
    buffer.clear();
    assertEquals(dataSize, IOUtils.read(mChannel, buffer));
    buffer.flip();
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, dataSize, buffer));

    buffer.clear();
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);
    buf.writerIndex(0);
    assertEquals(dataSize, mReader.transferTo(buf));
    assertTrue(BufferUtils.equalIncreasingByteBuffer(dataSize, dataSize, buffer));
  }
}
