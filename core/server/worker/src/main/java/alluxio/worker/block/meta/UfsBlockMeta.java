/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.meta;

import alluxio.thrift.LockBlockTOptions;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import java.io.IOException;

/**
 * This class represents the metadata of a UFS block.
 */
public final class UfsBlockMeta {
  private final long mSessionId;
  private final long mBlockId;
  private final String mUfsPath;
  /** The offset in bytes of the first byte of the block in its corresponding UFS file. */
  private final long mOffset;
  /** The block size in bytes. */
  private final long mBlockSize;

  /** The set of session IDs to be committed. */
  private boolean mCommitPending;
  private BlockReader mBlockReader;
  private BlockWriter mBlockWriter;

  public static UfsBlockMeta fromLockBlockOptions(long sessionId, long blockId,
      LockBlockTOptions options) {
    return new UfsBlockMeta(sessionId, blockId, options);
  }

  private UfsBlockMeta(long sessionId, long blockId, LockBlockTOptions options) {
    mSessionId = sessionId;
    mBlockId = blockId;
    mUfsPath = options.getUfsPath();
    mOffset = options.getOffset();
    mBlockSize = options.getBlockSize();
  }

  public long getSessionId() {
    return mSessionId;
  }

  public long getBlockId() {
    return mBlockId;
  }

  public String getUfsPath() {
    return mUfsPath;
  }

  public long getOffset() {
    return mOffset;
  }

  public long getBlockSize() {
    return mBlockSize;
  }

  public void closeReaderOrWriter() throws IOException {
    if (mBlockReader != null) {
      mBlockReader.close();
      mBlockReader = null;
    }
    if (mBlockWriter != null) {
      mBlockWriter.close();
      mBlockWriter = null;
    }
  }

  public BlockReader getBlockReader() {
    if (mBlockReader != null && mBlockReader.isClosed()) {
      mBlockReader = null;
    }
    return mBlockReader;
  }

  public BlockWriter getBlockWriter() {
    return mBlockWriter;
  }

  public boolean getCommitPending() {
    return mCommitPending;
  }

  public void setCommitPending(boolean commitPending) {
    mCommitPending = commitPending;
  }

  public void setBlockReader(BlockReader blockReader) {
    mBlockReader = blockReader;
  }

  public void setBlockWriter(BlockWriter blockWriter) {
    mBlockWriter = blockWriter;
  }
}

