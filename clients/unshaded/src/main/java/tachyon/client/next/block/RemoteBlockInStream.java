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
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import tachyon.client.RemoteBlockReader;
import tachyon.client.next.ClientContext;
import tachyon.client.next.ClientOptions;
import tachyon.thrift.FileBlockInfo;

/**
 * This class provides a streaming API to read a block in Tachyon. The data will be transferred
 * through a Tachyon worker's dataserver to the client.
 */
public class RemoteBlockInStream extends BlockInStream {
  private final long mBlockId;
  private final BSContext mContext;
  private final long mBlockSize;

  private long mPos;
  private String mRemoteHost;
  private int mRemotePort;

  // TODO: Make sure there is a valid Tachyon location
  // TODO: Modify the locking so the stream owns the lock instead of the data server
  public RemoteBlockInStream(FileBlockInfo blockInfo, ClientOptions options) {
    mBlockId = blockInfo.getBlockId();
    mContext = BSContext.INSTANCE;
    mBlockSize = blockInfo.getLength();
    // TODO: Clean this up
    mRemoteHost = blockInfo.getLocations().get(0).mHost;
    mRemotePort = blockInfo.getLocations().get(0).mPort;
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
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    } else if (mPos == mBlockSize) {
      return -1;
    }

    // We read at most len bytes, but if mPos + len exceeds the length of the block, we only
    // read up to the end of the block
    int lengthToRead = (int) Math.min(len, mBlockSize - mPos);
    int bytesLeft = lengthToRead;

    while (bytesLeft > 0) {
      // TODO: Fix needing to recreate reader each time
      RemoteBlockReader reader =
          RemoteBlockReader.Factory.createRemoteBlockReader(ClientContext.getConf());
      ByteBuffer data = reader.readRemoteBlock(mRemoteHost, mRemotePort, mBlockId, mPos, bytesLeft);
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
    Preconditions.checkArgument(pos > 0, "Seek position is negative: " + pos);
    Preconditions.checkArgument(pos < mBlockSize, "Seek position: " + pos + " is past block size: "
        + mBlockSize);
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    long skipped = Math.min(n, mBlockSize - mPos);
    mPos += skipped;
    return skipped;
  }
}
