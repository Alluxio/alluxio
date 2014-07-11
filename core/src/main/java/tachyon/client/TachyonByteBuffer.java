/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * TachyonByteBuffer is a wrapper on Java ByteBuffer plus some information needed by Tachyon.
 */
public class TachyonByteBuffer {
  // ByteBuffer contains data.
  public final ByteBuffer DATA;

  private final long BLOCK_ID;

  private final int BLOCK_LOCK_ID;

  private final TachyonFS TFS;

  private boolean mClosed = false;

  /**
   * @param tfs
   *          the Tachyon file system
   * @param buf
   *          the ByteBuffer wrapped on
   * @param blockId
   *          the id of the block
   * @param blockLockId
   *          the id of the block's lock
   */
  TachyonByteBuffer(TachyonFS tfs, ByteBuffer buf, long blockId, int blockLockId) {
    DATA = buf;
    BLOCK_ID = blockId;
    BLOCK_LOCK_ID = blockLockId;
    TFS = tfs;
  }

  /**
   * Close the TachyonByteBuffer, here it is synchronized
   * 
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }

    mClosed = true;
    if (BLOCK_LOCK_ID >= 0) {
      TFS.unlockBlock(BLOCK_ID, BLOCK_LOCK_ID);
    }
  }
}
