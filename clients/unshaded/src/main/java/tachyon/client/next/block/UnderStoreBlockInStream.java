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
import tachyon.underfs.UnderFileSystem;

import java.io.IOException;
import java.io.InputStream;

public class UnderStoreBlockInStream extends BlockInStream {
  private final String mUfsPath;

  private long mPos;
  private InputStream mUnderStoreStream;

  public UnderStoreBlockInStream(String ufsPath) throws IOException {
    mUfsPath = ufsPath;
    resetUnderStoreStream();
  }

  @Override
  public int read() throws IOException {
    int data = mUnderStoreStream.read();
    mPos ++;
    return data;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < mPos) {
      resetUnderStoreStream();
      if (skip(pos) != pos) {
        throw new IOException("Failed to seek backward to " + pos);
      }
    } else {
      if (skip(mPos - pos) != mPos - pos) {
        throw new IOException("Failed to seek forward to " + pos);
      }
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    int data = mUnderStoreStream.read(b);
    mPos ++;
    return data;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = mUnderStoreStream.read(b, off, len);
    mPos += bytesRead;
    return bytesRead;
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = mUnderStoreStream.skip(n);
    mPos += skipped;
    return skipped;
  }

  private void resetUnderStoreStream() throws IOException {
    if (null != mUnderStoreStream) {
      mUnderStoreStream.close();
    }
    UnderFileSystem ufs = UnderFileSystem.get(mUfsPath, ClientContext.getConf());
    mUnderStoreStream = ufs.open(mUfsPath);
    mPos = 0;
  }
}
