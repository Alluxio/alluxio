/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.block;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import tachyon.client.ClientContext;
import tachyon.client.RemoteBlockReader;

/**
 * This class provides a streaming API to read a block in Tachyon. The data will be transferred
 * through a Tachyon worker's dataserver to the client. The instances of this class should only be
 * used by one thread and are not thread safe.
 */
public final class RemoteBlockInStream extends BufferedBlockInStream {
  /**
   * Creates a new remote block input stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param location the location
   */
  // TODO(calvin): Modify the locking so the stream owns the lock instead of the data server.
  public RemoteBlockInStream(long blockId, long blockSize, InetSocketAddress location) {
    super(blockId, blockSize, location);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    // TODO(calvin): Perhaps verify that something was read from this stream
    ClientContext.getClientMetrics().incBlocksReadRemote(1);
    mClosed = true;
  }

  @Override
  protected void bufferedRead(int len) throws IOException {
    mBuffer.clear();
    readFromRemote(mBuffer.array(), 0, len);
  }

  @Override
  protected int directRead(byte[] b, int off, int len) throws IOException {
    return readFromRemote(b, off, len);
  }

  /**
   * Increments the number of bytes read metric.
   *
   * @param bytes number of bytes to record as read
   */
  @Override
  protected void incrementBytesReadMetric(int bytes) {
    ClientContext.getClientMetrics().incBytesReadRemote(bytes);
  }

  /**
   * Reads a portion of the block from the remote worker. This method does not modify mPos or
   * mBufferPos.
   *
   * @param b the byte array to write the data to
   * @param off the offset in the array to write to
   * @param len the length of data to write into the array
   * @return the number of bytes successfully read
   * @throws IOException if an error occurs reading the data
   */
  private int readFromRemote(byte[] b, int off, int len) throws IOException {
    // We read at most len bytes, but if mPos + len exceeds the length of the block, we only
    // read up to the end of the block.
    int toRead = (int) Math.min(len, remaining());
    int bytesLeft = toRead;
    while (bytesLeft > 0) {
      // TODO(calvin): Fix needing to recreate reader each time.
      RemoteBlockReader reader =
          RemoteBlockReader.Factory.createRemoteBlockReader(ClientContext.getConf());
      ByteBuffer data = reader.readRemoteBlock(mLocation, mBlockId, mPos, bytesLeft);
      int bytesRead = data.remaining();
      data.get(b, off, bytesRead);
      reader.close();
      bytesLeft -= bytesRead;
      incrementBytesReadMetric(bytesRead);
    }

    return toRead;
  }
}
