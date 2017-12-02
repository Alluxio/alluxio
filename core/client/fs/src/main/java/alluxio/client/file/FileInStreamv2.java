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

package alluxio.client.file;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.master.block.BlockId;
import alluxio.proto.dataserver.Protocol;
import com.google.common.base.Preconditions;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 *
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 */
@PublicApi
@NotThreadSafe
public class FileInStreamv2 extends InputStream implements Seekable {
  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  private final FileSystemContext mContext;
  private final AlluxioBlockStore mBlockStore;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;

  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;

  protected FileInStreamv2(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mOptions = options;
    mContext = context;
    mBlockStore = AlluxioBlockStore.create(context);

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();

    mPosition = 0;
  }

  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    updateStream();
    int result = mBlockInStream.read();
    if (result != -1) {
      mPosition++;
    }
    return result;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);

    if (mPosition == mLength) { // at end of file
      return -1;
    }

    int bytesLeft = len;
    int currentOffset = off;
    while (bytesLeft > 0 && mPosition != mLength) {
      updateStream();
      int bytesRead = mBlockInStream.read(b, currentOffset, bytesLeft);
      if (bytesRead > 0) {
        bytesLeft -= bytesRead;
        currentOffset += bytesRead;
        mPosition += bytesRead;
      }
    }
    return len - bytesLeft;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    closeBlockInStream();
  }

  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPosition == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);

    if (mBlockInStream == null) { // no current stream open, advance position
      mPosition += pos;
      return;
    }

    long delta = pos - mPosition;
    long fromBlockStart = Math.min(mLength, mBlockSize) - mBlockInStream.remaining();
    long fromBlockEnd = mBlockInStream.remaining();

    if (delta <= fromBlockEnd && delta >= -fromBlockStart) { // seek is within the current block
      mBlockInStream.seek(delta);
    } else { // close the underlying stream as the new position is no longer in bounds
      closeBlockInStream();
    }
    mPosition += delta;
  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream();
    }

    /* Create a new stream to read from mPosition. */
    // Calculate block id.
    int blockIndex = Math.toIntExact(mPosition / mBlockSize);
    long blockId = mStatus.getBlockIds().get(blockIndex);
    // If the file is persisted, provide the necessary info for the worker to fetch from UFS.
    Protocol.OpenUfsBlockOptions ufsOptions = null;
    if (mStatus.isPersisted()) {
      long blockOffset = BlockId.getSequenceNumber(blockId) * mStatus.getBlockSizeBytes();
      // Calculate the exact block size.
      long lastBlockSize = mLength % mBlockSize;
      long blockSize = mLength - blockOffset > lastBlockSize ? mBlockSize : lastBlockSize;

      ufsOptions =
          Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(mStatus.getUfsPath())
              .setOffsetInFile(blockOffset).setBlockSize(blockSize)
              .setMaxUfsReadConcurrency(mOptions.getMaxUfsReadConcurrency())
              .setNoCache(!mOptions.getAlluxioStorageType().isStore())
              .setMountId(mStatus.getMountId()).build();
    }

    mBlockInStream = mBlockStore.getInStream(blockId, ufsOptions, mOptions);
    // Set the stream to the correct position.
    long offset = mPosition % mBlockSize;
    mBlockInStream.seek(offset);
  }

  private void closeBlockInStream() throws IOException {
    mBlockInStream.close();
    // TODO(calvin): Handle async caching here
  }
}
