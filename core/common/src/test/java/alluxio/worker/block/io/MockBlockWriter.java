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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * A simple {@link BlockWriter} to use for testing purposes.
 */
public final class MockBlockWriter implements BlockWriter {
  private final ByteArrayOutputStream mOutputStream;

  /**
   * Constructs a mock block writer which will remember all bytes written to it.
   */
  public MockBlockWriter() {
    mOutputStream = new ByteArrayOutputStream();
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    byte[] bytes = new byte[inputBuf.remaining()];
    inputBuf.get(bytes);
    mOutputStream.write(bytes);
    return bytes.length;
  }

  @Override
  public WritableByteChannel getChannel() {
    return Channels.newChannel(mOutputStream);
  }

  /**
   * @return the bytes written to this writer
   */
  public byte[] getBytes() {
    return mOutputStream.toByteArray();
  }
}
