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

package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

/**
 * TachyonByteBuffer is a wrapper on Java ByteBuffer plus some information needed by Tachyon.
 */
public final class TachyonByteBuffer implements Closeable {
  // ByteBuffer contains data.
  public final ByteBuffer mData;

  private final long mBlockId;

  private final int mBlockLockId;

  private final TachyonFS mTachyonFS;

  private boolean mClosed = false;

  /**
   * @param tfs the Tachyon file system
   * @param buf the ByteBuffer wrapped on
   * @param blockId the id of the block
   * @param blockLockId the id of the block's lock
   */
  TachyonByteBuffer(TachyonFS tfs, ByteBuffer buf, long blockId, int blockLockId) {
    mTachyonFS = Preconditions.checkNotNull(tfs);
    mData = Preconditions.checkNotNull(buf);
    mBlockId = blockId;
    mBlockLockId = blockLockId;
  }

  /**
   * Closes the TachyonByteBuffer, here it is synchronized.
   *
   * @throws IOException when the underlying block cannot be unlocked
   */
  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }

    mClosed = true;
    if (mBlockLockId >= 0) {
      mTachyonFS.unlockBlock(mBlockId, mBlockLockId);
    }
  }
}
