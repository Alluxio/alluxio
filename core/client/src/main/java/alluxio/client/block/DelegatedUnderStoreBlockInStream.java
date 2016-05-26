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

package alluxio.client.block;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemWorkerClient;
import alluxio.client.file.UnderFileSystemFileInStream;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * Implementation of {@link UnderStoreBlockInStream} which accesses the under storage through an
 * Alluxio worker. Note that the seek implementation of this class is not fail fast, and seeking
 * past the end of the file will be successful until a subsequent read occurs.
 */
@NotThreadSafe
public final class DelegatedUnderStoreBlockInStream extends UnderStoreBlockInStream {
  /** File System Worker Client. */
  private final FileSystemWorkerClient mClient;
  /** File id of the ufs file. */
  private final long mUfsFileId;

  /**
   * Constructor for a direct under store block in stream.
   *
   * @param initPos position in the file
   * @param length length of the block
   * @param fileBlockSize file block size
   * @param ufsPath path in the ufs
   * @throws IOException if an error occurs initializing the stream to the ufs file
   */
  protected DelegatedUnderStoreBlockInStream(long initPos, long length, long fileBlockSize,
      String ufsPath) throws IOException {
    super(initPos, length, fileBlockSize, ufsPath);
    mClient = FileSystemContext.INSTANCE.createWorkerClient();
    try {
      mUfsFileId = mClient.openUfsFile(new AlluxioURI(ufsPath), OpenUfsFileOptions.defaults());
    } catch (AlluxioException | IOException e) {
      mClient.close();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    mUnderStoreStream.close();
    try {
      mClient.closeUfsFile(mUfsFileId, CloseUfsFileOptions.defaults());
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mClient.close();
    }
  }

  @Override
  protected void setUnderStoreStream(long pos) throws IOException {
    if (mUnderStoreStream != null) {
      mUnderStoreStream.close();
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_BLOCK.toString(), pos);
    mUnderStoreStream =
        new UnderFileSystemFileInStream(mClient.getWorkerDataServerAddress(), mUfsFileId);
    long streamStart = mInitPos + pos;
    // The stream is at the beginning of the file, so skip to the correct absolute position.
    if (streamStart != 0 && streamStart != mUnderStoreStream.skip(streamStart)) {
      mUnderStoreStream.close();
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(pos));
    }
    // Set the current block position to the specified block position.
    mPos = pos;
  }
}
