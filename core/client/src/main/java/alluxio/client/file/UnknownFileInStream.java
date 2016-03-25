/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.block.BlockInStream;
import alluxio.client.block.UnderStoreBlockInStream;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file of unknown length. A file of unknown length cannot be more than
 * one block.
 */
@PublicApi
@NotThreadSafe
public class UnknownFileInStream extends FileInStream {
  /**
   * Creates a new file input stream, for a file of unknown length.
   *
   * @param status the file status
   * @param options the client options
   */
  public UnknownFileInStream(URIStatus status, InStreamOptions options) {
    super(status, options);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mPos == mReadBytes && mCurrentBlockInStream != null
        && mCurrentBlockInStream.remaining() == 0) {
      // Every byte was read from the input stream. Therefore, the read bytes is the length.
      // Complete the file with this new, known length.
      FileSystemMasterClient masterClient = mContext.acquireMasterClient();
      try {
        masterClient.completeFile(new AlluxioURI(mStatus.getPath()),
            CompleteFileOptions.defaults().setUfsLength(mReadBytes));
      } catch (AlluxioException e) {
        throw new IOException(e);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    }
    super.close();
  }

  @Override
  public long remaining() {
    if (mCurrentBlockInStream != null) {
      return mCurrentBlockInStream.remaining();
    }
    // A file of unknown length can only be one block.
    return mBlockSize - mPos;
  }

  @Override
  protected boolean validPosition(long pos) {
    return pos < mBlockSize;
  }

  @Override
  protected long getBlockSizeAllocation(long pos) {
    // TODO(gpang): need a better way to handle unknown block size allocation
    // Since the file length is unknown, the actual length of the block is also unknown. Simply
    // using the maximum block size does not work well for allocation, since the maximum block
    // size for an unknown file may be large, and may not fit on any worker. Therefore, just
    // allow the allocation to succeed, but the cache out stream may fail later.
    return 128;
  }

  @Override
  protected BlockInStream createUnderStoreBlockInStream(long blockStart, long length, String path)
      throws IOException {
    return new UnderStoreBlockInStream(blockStart, Constants.UNKNOWN_SIZE, length, path);
  }

  @Override
  protected boolean shouldCloseCacheStream() {
    if (mCurrentCacheBytesWritten == mPos && mCurrentBlockInStream != null
        && mCurrentBlockInStream.remaining() == 0) {
      // Only close the cache stream if everything was read from the in stream, and everything
      // read was written to the cache out stream.
      return true;
    }
    return super.shouldCloseCacheStream();
  }

  @Override
  protected long getBlockSize(long pos) {
    return mBlockSize;
  }
}
