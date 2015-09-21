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

import tachyon.client.ClientContext;
import tachyon.client.RemoteBlockWriter;
import tachyon.worker.WorkerClient;

/**
 * Provides a streaming API to write to a Tachyon block. This output stream will send the write
 * through a Tachyon worker which will then write the block to a file in Tachyon storage. The
 * instances of this class should only be used by one thread and are not thread safe.
 */
public final class RemoteBlockOutStream extends BufferedBlockOutStream {
  private final RemoteBlockWriter mRemoteWriter;
  private final WorkerClient mWorkerClient;

  /**
   * Creates a new block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @throws IOException if I/O error occurs
   */
  public RemoteBlockOutStream(long blockId, long blockSize) throws IOException {
    super(blockId, blockSize);
    mRemoteWriter = RemoteBlockWriter.Factory.createRemoteBlockWriter(ClientContext.getConf());
    mWorkerClient = mContext.acquireWorkerClient();
    mRemoteWriter.open(mWorkerClient.getDataServerAddress(), mBlockId,
        mWorkerClient.getSessionId());
  }

  /**
   * Creates a new block output stream on a specific host.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param hostname the hostname of the preferred worker
   * @throws IOException if I/O error occurs
   */
  public RemoteBlockOutStream(long blockId, long blockSize, String hostname) throws IOException {
    super(blockId, blockSize);
    mRemoteWriter = RemoteBlockWriter.Factory.createRemoteBlockWriter(ClientContext.getConf());
    mWorkerClient = mContext.acquireWorkerClient(hostname);
    mRemoteWriter.open(mWorkerClient.getDataServerAddress(), mBlockId,
        mWorkerClient.getSessionId());
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mRemoteWriter.close();
    mWorkerClient.cancelBlock(mBlockId);
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    flush();
    mRemoteWriter.close();
    if (mFlushedBytes == mBlockSize) {
      mWorkerClient.cacheBlock(mBlockId);
      ClientContext.getClientMetrics().incBlocksWrittenRemote(1);
    } else {
      mWorkerClient.cancelBlock(mBlockId);
    }
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    writeToRemoteBlock(mBuffer.array(), 0, mBuffer.position());
    mBuffer.clear();
  }

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    writeToRemoteBlock(b, off, len);
  }

  private void writeToRemoteBlock(byte[] b, int off, int len) throws IOException {
    mRemoteWriter.write(b, off, len);
    mFlushedBytes += len;
    ClientContext.getClientMetrics().incBytesWrittenRemote(len);
  }
}
