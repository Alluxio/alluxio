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

import tachyon.client.next.ClientContext;
import tachyon.client.next.ClientOptions;
import tachyon.thrift.BlockInfo;

import java.io.IOException;

/**
 * This class provides a streaming API to read a block in Tachyon. The data will be transferred
 * through a Tachyon worker's dataserver to the client.
 */
public class RemoteBlockInStream extends BlockInStream {
  private final long mBlockId;
  private final BSContext mContext;
  private final long mBlockSize;
  private final boolean mCacheToLocal;

  private long mPos;
  private LocalBlockOutStream mCacheToLocalStream;

  public RemoteBlockInStream(BlockInfo blockInfo, ClientOptions options) {
    mBlockId = blockInfo.getBlockId();
    mContext = BSContext.INSTANCE;
    mBlockSize = blockInfo.getLength();
    mCacheToLocal = options.getCacheType().shouldCache() && mContext.hasLocalWorker();
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    if (read(b) == -1) {
      return -1;
    }
    // TODO: Move this logic to a utils class
    return (int) b[0] & 0xFF;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    // TODO: Implement me
  }
}
