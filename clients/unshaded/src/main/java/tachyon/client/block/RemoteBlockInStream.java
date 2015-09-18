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

import com.google.common.base.Preconditions;

import tachyon.client.ClientContext;
import tachyon.client.RemoteBlockReader;
import tachyon.thrift.NetAddress;
import tachyon.util.io.BufferUtils;
import tachyon.worker.WorkerClient;

/**
 * This class provides a streaming API to read a block in Tachyon. The data will be transferred
 * through a Tachyon worker's dataserver to the client. The instances of this class should only be
 * used by one thread and are not thread safe.
 */
public final class RemoteBlockInStream extends BlockInStream {
  private final long mBlockId;
  private final long mBlockSize;
  private final InetSocketAddress mLocation;
  private final WorkerClient mWorkerClient;

  private boolean mClosed;
  private long mPos;

  /**
   * Creates a new remote block input stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param location the location
   */
  public RemoteBlockInStream(long blockId, long blockSize, NetAddress location)
      throws IOException {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mClosed = false;
    // TODO(calvin): Validate these fields.
    mLocation = new InetSocketAddress(location.getHost(), location.getDataPort());
    mWorkerClient = BlockStoreContext.INSTANCE.acquireWorkerClient(location.getHost());
    String blockPath = null;
    try {
      blockPath = mWorkerClient.lockBlock(blockId);
    } catch (IOException ioe) {
      BlockStoreContext.INSTANCE.releaseWorkerClient(mWorkerClient);
      throw ioe;
    }

    if (blockPath == null) {
      // TODO(calvin): Handle this error case better.
      BlockStoreContext.INSTANCE.releaseWorkerClient(mWorkerClient);
      throw new IOException("Block is not available on remote machine: " + location.getHost());
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mWorkerClient.unlockBlock(mBlockId);
    BlockStoreContext.INSTANCE.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    checkIfClosed();
    byte[] b = new byte[1];
    if (read(b) == -1) {
      return -1;
    }
    return BufferUtils.byteToInt(b[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    checkIfClosed();
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(b != null, "Buffer is null");
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        String.format("Buffer length (%d), offset(%d), len(%d)", b.length, off, len));
    if (len == 0) {
      return 0;
    } else if (mPos == mBlockSize) {
      return -1;
    }

    // We read at most len bytes, but if mPos + len exceeds the length of the block, we only
    // read up to the end of the block.
    int lengthToRead = (int) Math.min(len, mBlockSize - mPos);
    int bytesLeft = lengthToRead;

    while (bytesLeft > 0) {
      // TODO(calvin): Fix needing to recreate reader each time.
      RemoteBlockReader reader =
          RemoteBlockReader.Factory.createRemoteBlockReader(ClientContext.getConf());
      ByteBuffer data = reader.readRemoteBlock(mLocation, mBlockId, mPos, bytesLeft);
      int bytesToRead = Math.min(bytesLeft, data.remaining());
      data.get(b, off, bytesToRead);
      reader.close();
      mPos += bytesToRead;
      bytesLeft -= bytesToRead;
    }

    return lengthToRead;
  }

  @Override
  public long remaining() {
    return mBlockSize - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(pos > 0, "Seek position is negative: " + pos);
    Preconditions.checkArgument(pos < mBlockSize,
        "Seek position: " + pos + " is past block size: " + mBlockSize);
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }
    long skipped = Math.min(n, mBlockSize - mPos);
    mPos += skipped;
    return skipped;
  }

  private void checkIfClosed() throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
  }
}
