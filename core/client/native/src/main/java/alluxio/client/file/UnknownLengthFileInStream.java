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

import alluxio.AlluxioURI;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file of unknown length. A file of unknown length cannot be more than
 * one block.
 *
 * TODO(gpang): This class should probably not implement BoundedStream, since remaining() does
 * not make sense for a file of unknown length. Investigate an alternative class hierarchy.
 */
@NotThreadSafe
public final class UnknownLengthFileInStream extends FileInStream {
  private static final Logger LOG = LoggerFactory.getLogger(UnknownLengthFileInStream.class);
  /**
   * Since the file length is unknown, the actual length of the block is also unknown. Simply
   * using the maximum block size does not work well for allocation, since the maximum block size
   * for an unknown file may be large, and may not fit on any worker. Therefore, use a smaller
   * allocation block size to allow the allocation to succeed, but the cache out stream may fail
   * later.
   */
  private static final long ALLOCATION_BLOCK_SIZE = 128;

  /**
   * Creates a new file input stream, for a file of unknown length.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   */
  public UnknownLengthFileInStream(URIStatus status, InStreamOptions options,
      FileSystemContext context) {
    super(status, options, context);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCurrentBlockInStream != null && inStreamRemaining() == 0) {
      // Every byte was read from the input stream. Therefore, the read bytes is the length.
      // Complete the file with this new, known length.
      FileSystemMasterClient masterClient = mContext.acquireMasterClient();
      try {
        masterClient.completeFile(new AlluxioURI(mStatus.getPath()),
            CompleteFileOptions.defaults().setUfsLength(mPos));
      } catch (AlluxioException e) {
        throw new IOException(e);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    } else {
      LOG.warn("File with unknown length was closed before fully reading the input stream.");
    }
    super.close();
  }

  @Override
  public long remaining() {
    if (mCurrentBlockInStream != null) {
      return inStreamRemaining();
    }
    // A file of unknown length can only be one block.
    return mBlockSize - mPos;
  }

  @Override
  protected long maxSeekPosition() {
    return mBlockSize;
  }

  @Override
  protected long getBlockSizeAllocation(long pos) {
    // TODO(gpang): need a better way to handle unknown block size allocation
    return ALLOCATION_BLOCK_SIZE;
  }

  @Override
  protected long getBlockSize(long pos) {
    return mBlockSize;
  }

  @Override
  protected boolean shouldUpdateStreams(long currentBlockId) {
    // Return true either at the beginning of a file or the end of a file.
    return mCurrentBlockInStream == null || inStreamRemaining() == 0;
  }
}
