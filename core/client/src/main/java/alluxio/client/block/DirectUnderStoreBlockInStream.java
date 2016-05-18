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

package alluxio.client.block;

import alluxio.client.ClientContext;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.UnderFileSystem;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * Implementation of {@link UnderStoreBlockInStream} which directly accesses the under storage
 * from the client.
 */
@NotThreadSafe
public final class DirectUnderStoreBlockInStream extends UnderStoreBlockInStream {
  /**
   * Constructor for a direct under store block in stream.
   *
   * @param initPos position in the file
   * @param length length of the block
   * @param fileBlockSize file block size
   * @param ufsPath path in the ufs
   * @throws IOException if an error occurs initializing the stream to the ufs file
   */
  protected DirectUnderStoreBlockInStream(long initPos, long length, long fileBlockSize,
      String ufsPath) throws IOException {
    super(initPos, length, fileBlockSize, ufsPath);
  }

  @Override
  protected void setUnderStoreStream(long pos) throws IOException {
    if (mUnderStoreStream != null) {
      mUnderStoreStream.close();
    }
    if (pos < 0 || pos > mLength) {
      throw new IOException(ExceptionMessage.FAILED_SEEK.getMessage(pos));
    }
    UnderFileSystem ufs = UnderFileSystem.get(mUfsPath, ClientContext.getConf());
    mUnderStoreStream = ufs.open(mUfsPath);
    // The stream is at the beginning of the file, so skip to the correct absolute position.
    long streamStart = mInitPos + pos;
    if (streamStart != 0 && streamStart != mUnderStoreStream.skip(streamStart)) {
      mUnderStoreStream.close();
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(pos));
    }
    // Set the current block position to the specified block position.
    mPos = pos;
  }
}
