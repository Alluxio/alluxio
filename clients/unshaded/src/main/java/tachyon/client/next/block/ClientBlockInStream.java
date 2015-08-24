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

import java.io.IOException;

import tachyon.client.next.ClientOptions;
import tachyon.thrift.FileBlockInfo;

/**
 * A wrapper on local, remote, or under file system streams. This class takes care of managing
 * which stream to use for reading and storing the file to local Tachyon storage.
 */
// TODO: Find a better name for this
public class ClientBlockInStream extends BlockInStream {
  private final FileBlockInfo mBlockInfo;
  private final BSContext mContext;

  private boolean mCacheToLocal;
  private BlockInStream mBlockInStream;
  private BlockOutStream mLocalBlockOutStream;

  public ClientBlockInStream(FileBlockInfo blockInfo, ClientOptions options) throws IOException {
    mBlockInfo = blockInfo;
    mContext = BSContext.INSTANCE;

    mCacheToLocal = options.getCacheType().shouldCache() && mContext.hasLocalWorker();

    // TODO: Determine the location to read from using some policy/conf values

    // TODO: It may be more efficient to check if local is in the block locations
    try {
      mBlockInStream = new LocalBlockInStream(mBlockInfo.getBlockId(), options);
      // Do not need to write to local since the data is already there.
      mCacheToLocal = false;
    } catch (IOException ioe) {
      // TODO: Log the error here
    }

    // Failed to get a local stream, try remote
    if (null == mBlockInStream) {
      mBlockInStream = new RemoteBlockInStream(mBlockInfo, options);
    }

    // Failed to get a remote stream, use under storage
    if (null == mBlockInStream) {
      // TODO: Investigate how to get the ufs path
      mBlockInStream = new UnderStoreBlockInStream(null);
    }

    if (mCacheToLocal) {
      // TODO: Investigate if this should be done lazily
      mLocalBlockOutStream = new LocalBlockOutStream(blockInfo.getBlockId(), options);
    }
  }

  @Override
  public void close() throws IOException {
    mLocalBlockOutStream.close();
    // TODO: Check that written bytes == block size
    mBlockInStream.close();
  }

  @Override
  public int read() throws IOException {
    int data = mBlockInStream.read();
    // TODO: Investigate asnyc cache to local
    if (mCacheToLocal) {
      mLocalBlockOutStream.write(data);
    }
    return data;
  }

  @Override
  public int read(byte[] b) throws IOException {
    int bytesRead = mBlockInStream.read(b);
    if (mCacheToLocal) {
      mLocalBlockOutStream.write(b);
    }
    return bytesRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = mBlockInStream.read(b, off, len);
    if (mCacheToLocal) {
      mLocalBlockOutStream.write(b, off, len);
    }
    return bytesRead;
  }

  @Override
  public void seek(long pos) throws IOException {
    // No longer cache the block if the read is not a full sequential read
    cancelCacheToLocal();

    mBlockInStream.seek(pos);
  }

  @Override
  public long skip(long n) throws IOException {
    // No longer cache the block if the read is not a full sequential read
    cancelCacheToLocal();

    return mBlockInStream.skip(n);
  }

  private void cancelCacheToLocal() throws IOException {
    mCacheToLocal = false;
    if (null != mLocalBlockOutStream) {
      mLocalBlockOutStream.cancel();
    }
  }
}
