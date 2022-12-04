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

package alluxio.worker.dora;

import alluxio.worker.block.io.BlockReader;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Paged file reader.
 */
public class PagedFileReader extends BlockReader {
  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    return null;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public ReadableByteChannel getChannel() {
    return null;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public String getLocation() {
    return null;
  }
}
