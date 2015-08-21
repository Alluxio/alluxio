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

package tachyon.client.next.block;

import com.google.common.base.Preconditions;
import tachyon.Constants;
import tachyon.client.RemoteBlockWriter;
import tachyon.client.next.ClientContext;
import tachyon.client.next.ClientOptions;
import tachyon.util.io.BufferUtils;
import tachyon.worker.next.WorkerClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Provides a streaming API to write to a Tachyon block. This output stream will send the write
 * through a Tachyon worker which will then write the block to a file in Tachyon storage.
 */
public class RemoteBlockOutStream extends BlockOutStream {
  private final long mBlockId;
  private final long mBlockSize;
  private final BSContext mContext;
  private final ByteBuffer mBuffer;
  private final RemoteBlockWriter mRemoteWriter;
  private final WorkerClient mWorkerClient;

  private boolean mClosed;
  private long mFlushedBytes;
  private long mWrittenBytes;

  public RemoteBlockOutStream(long blockId, ClientOptions options) throws IOException {
    Preconditions.checkArgument(!options.getCacheType().shouldCache(), "Remote Block OutStream "
        + "only supports CacheType CACHE.");
    mBlockId = blockId;
    mBlockSize = options.getBlockSize();
    mContext = BSContext.INSTANCE;
    // TODO: Get this value from the conf
    mBuffer = ByteBuffer.allocate(Constants.MB * 8);
    mRemoteWriter = RemoteBlockWriter.Factory.createRemoteBlockWriter(ClientContext.getConf());
    // TODO: This should be specified outside of options
    InetSocketAddress workerAddr =
        new InetSocketAddress(options.getLocation().getMHost(), options.getLocation()
            .getMSecondaryPort());
    mWorkerClient = mContext.acquireWorkerClient(workerAddr.getHostName());
    // TODO: Get the user ID
    mRemoteWriter.open(workerAddr, mBlockId, 1);
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
    mRemoteWriter.close();
    if (mFlushedBytes == mBlockSize) {
      mWorkerClient.cacheBlock(mBlockId);
    } else {
      mWorkerClient.cancelBlock(mBlockId);
    }
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public void write(int b) throws IOException {
    failIfClosed();
    if (mWrittenBytes + 1 > mBlockSize) {
      throw new IOException("Out of capacity.");
    }
    if (mBuffer.position() >= mBuffer.limit()) {
      flushBufferToRemote();
    }
    BufferUtils.putIntByteBuffer(mBuffer, b);
    mWrittenBytes ++;
  }

  private void failIfClosed() throws IOException {
    if (mClosed) {
      throw new IOException("Cannot do operations on a closed RemoteBlockOutStream");
    }
  }

  private void flushBufferToRemote() throws IOException {
    mRemoteWriter.write(mBuffer.array(), 0, mBuffer.position());
    mFlushedBytes += mBuffer.position();
    mBuffer.clear();
  }
}
