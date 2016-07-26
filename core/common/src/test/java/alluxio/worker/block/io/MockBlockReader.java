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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * A simple {@link BlockReader} to use for testing purposes.
 */
public final class MockBlockReader implements BlockReader {
  private final byte[] mBytes;

  /**
   * Constructs a mock block reader which will read the given data.
   *
   * @param bytes the bytes to read from
   */
  public MockBlockReader(byte[] bytes) {
    mBytes = bytes;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    return ByteBuffer.wrap(mBytes, (int) offset, (int) length);
  }

  @Override
  public long getLength() {
    return mBytes.length;
  }

  @Override
  public ReadableByteChannel getChannel() {
    return Channels.newChannel(new ByteArrayInputStream(mBytes));
  }
}
