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
import java.nio.ByteBuffer;

import com.google.common.io.Closer;

import tachyon.client.ClientContext;
import tachyon.client.worker.BlockWorkerClient;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.thrift.LockBlockResult;
import tachyon.util.io.BufferUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.block.io.LocalFileBlockReader;

/**
 * This class provides a streaming API to read a block in Tachyon. The data will be directly read
 * from the local machine's storage. The instances of this class should only be used by one thread
 * and are not thread safe.
 */
public final class LocalBlockInStream extends BufferedBlockInStream {
  /** Helper to manage closables. */
  private final Closer mCloser;
  /** Client to communicate with the local worker. */
  private final BlockWorkerClient mBlockWorkerClient;
  /** The block store context which provides block worker clients. */
  private final BlockStoreContext mContext;
  /** The file reader to read a local block */
  private final LocalFileBlockReader mReader;

  /**
   * Creates a new local block input stream.
   *
   * @param blockId the block id
   * @param blockSize the size of the block
   * @throws IOException if I/O error occurs
   */
  public LocalBlockInStream(long blockId, long blockSize) throws IOException {
    super(blockId, blockSize);
    mContext = BlockStoreContext.INSTANCE;

    mCloser = Closer.create();
    mBlockWorkerClient =
        mContext.acquireWorkerClient(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));
    try {
      LockBlockResult result = mBlockWorkerClient.lockBlock(blockId);
      if (result == null) {
        throw new IOException(ExceptionMessage.BLOCK_NOT_LOCALLY_AVAILABLE.getMessage(mBlockId));
      }
      mReader = new LocalFileBlockReader(result.blockPath);
      mCloser.register(mReader);
    } catch (IOException e) {
      mContext.releaseWorkerClient(mBlockWorkerClient);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      if (mBlockIsRead) {
        mBlockWorkerClient.accessBlock(mBlockId);
        ClientContext.getClientMetrics().incBlocksReadLocal(1);
      }
      mBlockWorkerClient.unlockBlock(mBlockId);
    } catch (TachyonException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseWorkerClient(mBlockWorkerClient);
      mCloser.close();
      if (mBuffer != null && mBuffer.isDirect()) {
        BufferUtils.cleanDirectBuffer(mBuffer);
      }
    }

    mClosed = true;
  }

  @Override
  protected void bufferedRead(int len) throws IOException {
    if (mBuffer.isDirect()) { // Buffer may not be direct on initialization
      BufferUtils.cleanDirectBuffer(mBuffer);
    }
    mBuffer = mReader.read(getPosition(), len);
  }

  @Override
  public int directRead(byte[] b, int off, int len) throws IOException {
    ByteBuffer buf = mReader.read(getPosition(), len);
    buf.get(b, off, len);
    BufferUtils.cleanDirectBuffer(buf);
    return len;
  }

  @Override
  protected void incrementBytesReadMetric(int bytes) {
    ClientContext.getClientMetrics().incBytesReadLocal(bytes);
  }
}
